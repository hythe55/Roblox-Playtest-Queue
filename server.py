#!/usr/bin/env python3
"""Small stdio MCP server that shares one Roblox Studio between agents using lanes.

Every Claude Code session spawns its own copy of this process; all copies share one
sqlite database. A job is only ever granted by its own polling thread (inside an
`acquire` call), so only a live waiter can receive a lease.
"""
import json, logging, os, re, sqlite3, sys, threading, time, uuid
from contextlib import contextmanager
from audio import mute_studio, restore_studio

VERSION = "0.3.0"


def _env(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


DEFAULT_DB_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "RobloxPlaytestQueue")
DB = os.environ.get("ROBLOX_PLAYTEST_QUEUE_DB", os.path.join(DEFAULT_DB_DIR, "queue.db"))
LOG_FILE = os.environ.get("ROBLOX_PLAYTEST_QUEUE_LOG", os.path.join(os.path.dirname(__file__), "queue.log"))
LEASE_SECONDS = _env("ROBLOX_PLAYTEST_LEASE_SECONDS", 300)        # play / edit lease and renew length
CAMERA_SECONDS = _env("ROBLOX_PLAYTEST_CAMERA_SECONDS", 120)      # camera lease, capped at 120, not renewable
QUEUE_WAIT_SECONDS = _env("ROBLOX_PLAYTEST_QUEUE_WAIT_SECONDS", 3600)  # queued jobs older than this expire
MAX_WAIT_SECONDS = _env("ROBLOX_PLAYTEST_ACQUIRE_MAX_WAIT_SECONDS", 270)  # one acquire call blocks at most this long
GRACE_SECONDS = _env("ROBLOX_PLAYTEST_GRACE_SECONDS", 120)        # a queued job keeps its place this long between calls
LIVE_SECONDS = _env("ROBLOX_PLAYTEST_LIVE_SECONDS", 30)           # a waiter unseen this long no longer blocks others
POLL_SECONDS = _env("ROBLOX_PLAYTEST_POLL_SECONDS", 2)
HEARTBEAT_SECONDS = _env("ROBLOX_PLAYTEST_HEARTBEAT_SECONDS", 10)
PROCESS_TIMEOUT_SECONDS = _env("ROBLOX_PLAYTEST_PROCESS_TIMEOUT_SECONDS", 60)
NOTES_WINDOW_SECONDS = 30 * 60
MAX_REJOIN_SECONDS = 300
MAX_CAMERA_SECONDS = 120
LANES = ("play", "edit", "camera")
OLD_STATES = ("queued", "active", "released", "expired")  # states an old-code process understands

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
reply_lock = threading.Lock()
PROC_ID = uuid.uuid4().hex  # None means "exempt from dead-process cleanup" (manual CLI leases)
INFLIGHT = {}               # JSON-RPC request id -> Ctx, for notifications/cancelled
JOB_CTX = {}                # job_id -> Ctx of the thread polling for it in this process
_migrated = set()
_migrate_lock = threading.Lock()
_hb_lock = threading.Lock()
_hb_thread = None


class Ctx:
    """Per-acquire-call state shared between the polling thread and cancel paths."""
    def __init__(self):
        self.cancelled = threading.Event()
        self.wake = threading.Event()
        self.silent = False
        self.agent = None
        self.job_id = None


# ---------------------------------------------------------------- database

NEW_JOB_COLUMNS = [("lane", "TEXT"), ("scope", "TEXT"), ("purpose", "TEXT"), ("minutes", "REAL"),
                   ("proc", "TEXT"), ("last_seen", "REAL"), ("queued_at", "REAL"), ("notes", "TEXT"),
                   ("rejoin_until", "REAL")]


def migrate(c):
    c.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, agent TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL, started REAL, lease_until REAL, released REAL, UNIQUE(id))")
    have = {r[1] for r in c.execute("PRAGMA table_info(jobs)")}
    for name, typ in NEW_JOB_COLUMNS:
        if name not in have:
            try:
                c.execute(f"ALTER TABLE jobs ADD COLUMN {name} {typ}")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
    c.execute("CREATE TABLE IF NOT EXISTS processes (id TEXT PRIMARY KEY, pid INTEGER, started REAL, last_seen REAL NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS flags (name TEXT PRIMARY KEY, agent TEXT, reason TEXT, at REAL)")


def db():
    path = os.path.abspath(DB)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    c = sqlite3.connect(path, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    with _migrate_lock:
        if path not in _migrated:
            migrate(c)
            _migrated.add(path)
    return c


@contextmanager
def txn(c):
    c.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        c.execute("ROLLBACK")
        raise
    else:
        c.execute("COMMIT")


def touch_process(c, now=None):
    if PROC_ID is None:
        return
    now = now or time.time()
    c.execute("INSERT INTO processes(id,pid,started,last_seen) VALUES(?,?,?,?) "
              "ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen", (PROC_ID, os.getpid(), now, now))


def _heartbeat_loop():
    while True:
        try:
            c = db()
            try:
                with txn(c): touch_process(c)
            finally:
                c.close()
        except Exception:
            logging.exception("heartbeat failed")
        time.sleep(HEARTBEAT_SECONDS)


def ensure_process(c=None):
    """Register this process and start its heartbeat thread (once)."""
    global _hb_thread
    if PROC_ID is None:
        return
    with _hb_lock:
        if _hb_thread is None:
            own = c is None
            c = c or db()
            try:
                with txn(c): touch_process(c)
            finally:
                if own: c.close()
            _hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="queue-heartbeat")
            _hb_thread.start()


def set_exempt_process():
    """For the manual CLI: its leases must outlive the process, so no heartbeat and no proc id."""
    global PROC_ID
    PROC_ID = None


# ---------------------------------------------------------------- lane logic

def lane_of(r):
    return r["lane"] or "play"


def scope_of(r):
    if not r["scope"]:
        return []
    try:
        v = json.loads(r["scope"])
        return [s for s in v if isinstance(s, str) and s] if isinstance(v, list) else []
    except ValueError:
        return []


def paths_overlap(a, b):
    a, b = a.casefold(), b.casefold()
    return a == b or b.startswith(a + ".") or a.startswith(b + ".")


def scopes_overlap(sa, sb):
    if not sa or not sb:  # missing scope means the whole place
        return True
    return any(paths_overlap(x, y) for x in sa for y in sb)


def conflicts(a, b):
    la, lb = lane_of(a), lane_of(b)
    if la == "play" or lb == "play":
        return True
    if la == "camera" and lb == "camera":
        return True
    if la == "edit" and lb == "edit":
        return scopes_overlap(scope_of(a), scope_of(b))
    return False  # camera vs edit


def order_key(r):
    return (r["created"], r["id"])


def is_live(r, now):
    return r["last_seen"] is None or now - r["last_seen"] <= LIVE_SECONDS


def active_rows(c, now):
    return c.execute("SELECT * FROM jobs WHERE state='active' AND lease_until >= ?", (now,)).fetchall()


def queued_rows(c):
    return sorted(c.execute("SELECT * FROM jobs WHERE state='queued'").fetchall(), key=order_key)


def blockers_for(c, row, now):
    """Active leases that conflict with `row`, and live earlier-queued jobs that conflict with it."""
    act = [a for a in active_rows(c, now) if a["id"] != row["id"] and conflicts(a, row)]
    ahead = [q for q in queued_rows(c) if q["id"] != row["id"] and order_key(q) < order_key(row)
             and is_live(q, now) and conflicts(q, row)]
    return act, ahead


def fmt_dur(s):
    s = max(0, int(s))
    if s < 90: return f"{s}s"
    if s < 3600: return f"{s // 60}m"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def fmt_scope(r):
    if lane_of(r) != "edit": return "-"
    s = scope_of(r)
    return ",".join(s) if s else "*"


def one_line(s, n=120):
    return re.sub(r"\s+", " ", s or "").strip()[:n]


def log_fields(r):
    return f"lane={lane_of(r)} scope={fmt_scope(r)} purpose={one_line(r['purpose'], 80) or '-'}"


def fmt_eta(c, row, now):
    act, ahead = blockers_for(c, row, now)
    if not act and not ahead:
        return "next poll"
    total = 0.0
    for b in act + ahead:
        m = b["minutes"]
        if m is None:
            return "unknown"
        total += max(0.0, m - (now - (b["started"] or now)) / 60) if b["state"] == "active" else m
    return f"~{max(1, round(total))} min"


def position(c, row):
    q = queued_rows(c)
    for i, r in enumerate(q):
        if r["id"] == row["id"]:
            return i + 1, len(q)
    return 0, len(q)


def end_job(r, state, now, events, restore, c, reason, event):
    """Move a queued or active row to a finished state. Must run inside a transaction."""
    if r["proc"] is None and state not in OLD_STATES:
        state = "expired"  # rows from old-code processes only understand the old states
    n = c.execute("UPDATE jobs SET state=?, released=? WHERE id=? AND state IN ('queued','active')",
                  (state, now, r["id"])).rowcount
    if not n:
        return False
    if r["state"] == "active" and lane_of(r) == "play":
        restore.append(r["id"])
    events.append((logging.INFO, f"{event} job={r['id']} agent={r['agent']} {log_fields(r)} was={r['state']} reason={reason}"))
    return True


def flush(events, restore):
    for level, msg in events:
        logging.log(level, msg)
    for jid in restore:
        try:
            restore_studio(jid)
        except Exception:
            logging.exception("audio restore failed job=%s", jid)


def cleanup(c):
    now = time.time(); events = []; restore = []
    with txn(c):
        for p in c.execute("SELECT id FROM processes WHERE last_seen < ?", (now - PROCESS_TIMEOUT_SECONDS,)).fetchall():
            for r in c.execute("SELECT * FROM jobs WHERE proc=? AND state IN ('queued','active')", (p["id"],)).fetchall():
                end_job(r, "expired", now, events, restore, c, "process-dead", "expire")
            c.execute("DELETE FROM processes WHERE id=?", (p["id"],))
        for r in c.execute("SELECT * FROM jobs WHERE state='active' AND lease_until < ?", (now,)).fetchall():
            end_job(r, "expired", now, events, restore, c, "lease-ended", "expire")
        for r in c.execute("SELECT * FROM jobs WHERE state='queued' AND created < ?", (now - QUEUE_WAIT_SECONDS,)).fetchall():
            end_job(r, "expired", now, events, restore, c, "queue-timeout", "expire")
        for r in c.execute("SELECT * FROM jobs WHERE state='queued' AND last_seen IS NOT NULL AND last_seen < ?",
                           (now - GRACE_SECONDS,)).fetchall():
            end_job(r, "abandoned", now, events, restore, c, "not-recalled", "abandon")
    flush(events, restore)


def wake_all():
    for ctx in list(JOB_CTX.values()):
        ctx.wake.set()


def down_flag(c):
    return c.execute("SELECT * FROM flags WHERE name='studio_down'").fetchone()


def down_text(f):
    return (f"Studio reported down by {f['agent']} at {time.strftime('%Y-%m-%d %H:%M', time.localtime(f['at']))}: "
            f"{f['reason']}. Stop and tell your orchestrator or user.")


# ---------------------------------------------------------------- argument parsing

def need_str(args, key, tool):
    v = args.get(key)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{tool} requires {'a stable ' if key == 'job_id' else ''}{key}")
    return v


def parse_scope(v):
    if v is None: return []
    if isinstance(v, str): v = [v]
    if not isinstance(v, list): raise ValueError("scope must be a list of dotted instance paths")
    out = []
    for s in v:
        if not isinstance(s, str): raise ValueError("scope entries must be strings")
        s = s.strip().strip(".")
        if s and s.casefold() not in [o.casefold() for o in out]:
            out.append(s)
    return out


def parse_minutes(v):
    if v is None or v == "": return None
    try:
        m = float(v)
    except (TypeError, ValueError):
        raise ValueError("minutes must be a number")
    return m if m > 0 else None


def holding_note(c, agent, job_id, now):
    mine = [r for r in active_rows(c, now) if r["agent"] == agent and r["id"] != job_id]
    if not mine:
        return ""
    return (" Note: you still hold an active lease (" + ", ".join(f"job_id={r['id']} lane={lane_of(r)}" for r in mine) +
            "); it is untouched, and a conflicting request waits on it until you release it.")


# ---------------------------------------------------------------- acquire

def granted(c, row, now):
    lane = lane_of(row)
    parts = [f"Ready. Use the same agent and job_id={row['id']} when releasing or renewing. "
             f"lane={lane}{'' if lane != 'edit' else ' scope=' + fmt_scope(row)} lease={row['lease_until'] - now:.0f}s."]
    if lane == "camera":
        parts.append("Camera lease is short and not renewable; release as soon as the capture is done.")
    elif lane == "edit":
        parts.append("Stay inside your scope. Renew before expiry; if renew says a play request is waiting, release at the next safe point.")
    if lane == "play":
        n = c.execute("SELECT * FROM jobs WHERE notes IS NOT NULL AND notes != '' AND agent != ? AND released >= ? "
                      "ORDER BY released DESC LIMIT 1", (row["agent"], now - NOTES_WINDOW_SECONDS)).fetchone()
        if n:
            parts.append(f"Previous holder {n['agent']} ({lane_of(n)}, {fmt_dur(now - n['released'])} ago) left notes: {n['notes']}")
        parts.append("Before starting, make sure Studio is stopped and in the state your project's docs require.")
    note = holding_note(c, row["agent"], row["id"], now)
    if note: parts.append(note.strip())
    return {"status": "granted", "job_id": row["id"], "lease_id": row["id"], "lane": lane,
            "expires_at": row["lease_until"], "lease_seconds": row["lease_until"] - now, "text": " ".join(parts)}


def acquire(args, ctx=None):
    agent = need_str(args, "agent", "acquire")
    job_id = need_str(args, "job_id", "acquire")
    lane = args.get("lane") or "play"
    if lane not in LANES: raise ValueError(f"lane must be one of {', '.join(LANES)}")
    scope = parse_scope(args.get("scope")) if lane == "edit" else []
    purpose = one_line(args.get("purpose")) or None
    minutes = parse_minutes(args.get("minutes"))
    ctx = ctx or Ctx()
    ctx.agent, ctx.job_id = agent, job_id
    if ctx.cancelled.is_set():
        return {"status": "cancelled", "job_id": job_id, "text": f"Cancelled. job_id={job_id}"}
    JOB_CTX[job_id] = ctx
    c = db()
    try:
        ensure_process(c)
        deadline = time.time() + MAX_WAIT_SECONDS
        first = True
        while True:
            if ctx.cancelled.is_set():
                return {"status": "cancelled", "job_id": job_id, "text": f"Cancelled. job_id={job_id}"}
            cleanup(c)
            ctx.wake.clear()
            events = []; restore = []; result = None; mute = False
            with txn(c):
                now = time.time()
                touch_process(c, now)
                row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                down = down_flag(c)
                if down and not (row and row["state"] == "active"):
                    if row and row["state"] == "queued":
                        end_job(row, "cancelled", now, events, restore, c, "studio-down", "cancel")
                    result = {"status": "down", "job_id": job_id, "text": down_text(down)}
                elif down:
                    result = {"status": "down", "job_id": job_id, "text": down_text(down) + " Your current lease continues; finish and release it."}
                if result is None and row is None:
                    if not first: raise ValueError("job not found")
                    for old in c.execute("SELECT * FROM jobs WHERE agent=? AND state='queued'", (agent,)).fetchall():
                        if end_job(old, "superseded", now, events, restore, c, f"new-job={job_id}", "supersede"):
                            oc = JOB_CTX.get(old["id"])
                            if oc: oc.wake.set()
                    created = now
                    rj = c.execute("SELECT * FROM jobs WHERE agent=? AND state='released' AND rejoin_until >= ? "
                                   "ORDER BY released DESC LIMIT 1", (agent, now)).fetchone()
                    if rj:  # stepped out with rejoin_seconds: take the old queue time (clamped so old code's age expiry keeps it)
                        created = min(now, max(rj["created"], now - QUEUE_WAIT_SECONDS / 2))
                    c.execute("UPDATE jobs SET rejoin_until=NULL WHERE agent=? AND rejoin_until IS NOT NULL", (agent,))
                    c.execute("INSERT INTO jobs(id,agent,state,created,lane,scope,purpose,minutes,proc,last_seen,queued_at) "
                              "VALUES(?,?, 'queued',?,?,?,?,?,?,?,?)",
                              (job_id, agent, created, lane, json.dumps(scope) if scope else None, purpose, minutes, PROC_ID, now, now))
                    row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                if result is None:
                    st = row["state"]
                    if st == "active":
                        result = granted(c, row, now)
                    elif st != "queued":
                        if first:
                            raise ValueError(f"job {job_id} is already {st}; use a new job_id")
                        msgs = {"superseded": "Superseded by a newer acquire from this agent; nothing to release.",
                                "cancelled": "Cancelled.", "abandoned": "Abandoned (not re-called in time).",
                                "released": "Already released.", "expired": "Expired."}
                        result = {"status": st, "job_id": job_id, "text": f"{msgs.get(st, st)} job_id={job_id}. Use a new job_id to queue again."}
                    else:
                        c.execute("UPDATE jobs SET last_seen=? WHERE id=?", (now, job_id))
                        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                        act, ahead = blockers_for(c, row, now)
                        if not act and not ahead:
                            secs = min(CAMERA_SECONDS, MAX_CAMERA_SECONDS) if lane_of(row) == "camera" else LEASE_SECONDS
                            c.execute("UPDATE jobs SET state='active',started=?,lease_until=? WHERE id=? AND state='queued'",
                                      (now, now + secs, job_id))
                            row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                            mute = lane_of(row) == "play"
                            result = granted(c, row, now)
                            events.append((logging.INFO, f"acquire job={job_id} agent={agent} {log_fields(row)}"))
                        elif time.time() >= deadline:
                            pos, total = position(c, row)
                            eta = fmt_eta(c, row, now)
                            result = {"status": "queued", "job_id": job_id, "position": pos, "queued": total, "eta": eta,
                                      "text": (f"Still queued (not an error): position {pos} of {total}, ETA {eta}. "
                                               f"Call acquire again now with the same agent and job_id={job_id} "
                                               f"(your place is kept for {GRACE_SECONDS:.0f}s)." + holding_note(c, agent, job_id, now))}
            flush([e for e in events if not e[1].startswith("acquire ")], restore)
            if mute:
                audio = mute_studio(job_id)
                logging.info("audio mute job=%s enabled=%s muted=%s watching=%s", job_id, audio.get("enabled"), audio.get("muted"), audio.get("watching"))
                if audio.get("warning"):
                    logging.warning("audio mute job=%s warning=%s", job_id, audio["warning"])
            flush([e for e in events if e[1].startswith("acquire ")], [])
            if result is not None:
                return result
            first = False
            ctx.wake.wait(max(0.0, min(POLL_SECONDS, deadline - time.time())) or 0.001)
    finally:
        if JOB_CTX.get(job_id) is ctx:
            JOB_CTX.pop(job_id, None)
        c.close()


# ---------------------------------------------------------------- release / renew / cancel

def release(args):
    agent = need_str(args, "agent", "release")
    job_id = need_str(args, "job_id", "release")
    notes = one_line(args.get("notes"), 1000) or None
    rj = args.get("rejoin_seconds")
    try:
        rj = 0 if rj in (None, "") else float(rj)
    except (TypeError, ValueError):
        raise ValueError("rejoin_seconds must be a number from 0 to 300")
    if not 0 <= rj <= MAX_REJOIN_SECONDS:
        raise ValueError("rejoin_seconds must be between 0 and 300")
    c = db(); events = []; restore = []
    try:
        with txn(c):
            now = time.time()
            row = c.execute("SELECT * FROM jobs WHERE id=? AND state='active' AND agent=?", (job_id, agent)).fetchone()
            if not row:
                raise ValueError("job_id is not an active lease owned by this agent (it may have expired, or been cancelled); nothing to release")
            c.execute("UPDATE jobs SET state='released', released=?, notes=COALESCE(?, notes), rejoin_until=? WHERE id=?",
                      (now, notes, now + rj if rj > 0 else None, job_id))
            if lane_of(row) == "play": restore.append(job_id)
            events.append((logging.INFO, f"release job={job_id} agent={agent} {log_fields(row)}"))
    finally:
        c.close()
    flush(events, restore); wake_all()
    extra = f" Your place is held for {rj:.0f}s: acquire again within that time to go to the front." if rj > 0 else ""
    return {"released": True, "job_id": job_id, "text": f"Lease released. job_id={job_id}.{extra}"}


def renew(args):
    agent = need_str(args, "agent", "renew")
    job_id = need_str(args, "job_id", "renew")
    c = db(); msgs = []; yield_now = False
    try:
        with txn(c):
            now = time.time()
            row = c.execute("SELECT * FROM jobs WHERE id=? AND state='active' AND lease_until >= ? AND agent=?", (job_id, now, agent)).fetchone()
            if not row:
                raise ValueError("lease is missing or expired")
            lane = lane_of(row)
            if lane == "camera":
                raise ValueError("camera leases are not renewable; release and acquire again")
            waiting = [q for q in queued_rows(c) if is_live(q, now) and conflicts(q, row)]
            if lane == "edit":
                plays = [q for q in queued_rows(c) if is_live(q, now) and lane_of(q) == "play"]
                if plays:
                    waited = now - (plays[0]["queued_at"] or plays[0]["created"])
                    if waited > LEASE_SECONDS:
                        raise ValueError(f"Renew refused: a play request ({plays[0]['agent']}) has waited {fmt_dur(waited)}. "
                                         "Finish and release now, put Studio in a clean state and say so in notes.")
                    yield_now = True
                    msgs.append(f"A play request ({plays[0]['agent']}) is waiting: release at the next safe point. Renews are refused once it has waited {fmt_dur(LEASE_SECONDS)}.")
            if row["minutes"] and row["started"] and now - row["started"] > 1.5 * row["minutes"] * 60:
                pct = round(100 * (now - row["started"]) / (row["minutes"] * 60))
                msgs.append(f"You are at {pct}% of your {row['minutes']:g} min estimate; {len(waiting)} waiting.")
            until = now + LEASE_SECONDS
            c.execute("UPDATE jobs SET lease_until=? WHERE id=?", (until, job_id))
    finally:
        c.close()
    return {"job_id": job_id, "expires_at": until, "lease_seconds": LEASE_SECONDS, "yield": yield_now,
            "text": f"Lease renewed. job_id={job_id} expires_at={until:.0f} (+{LEASE_SECONDS:.0f}s)." + (" " + " ".join(msgs) if msgs else "")}


def cancel(args):
    agent = need_str(args, "agent", "cancel")
    job_id = need_str(args, "job_id", "cancel")
    c = db(); events = []; restore = []
    try:
        with txn(c):
            now = time.time()
            row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                return {"cancelled": False, "job_id": job_id, "text": f"No such job_id={job_id}; nothing to cancel."}
            if row["agent"] != agent:
                raise ValueError("job_id belongs to another agent")
            if row["state"] not in ("queued", "active"):
                return {"cancelled": False, "job_id": job_id, "text": f"job_id={job_id} is already {row['state']}; nothing to cancel."}
            was = row["state"]
            end_job(row, "released" if was == "active" else "cancelled", now, events, restore, c, "cancel", "cancel")
    finally:
        c.close()
    flush(events, restore)
    ctx = JOB_CTX.get(job_id)
    if ctx: ctx.wake.set()
    wake_all()
    return {"cancelled": True, "job_id": job_id, "text": f"{'Lease released' if was == 'active' else 'Queued request cancelled'}. job_id={job_id}"}


def shutdown_process():
    """On stdin EOF: release and cancel everything this process owns."""
    if PROC_ID is None: return
    c = db(); events = []; restore = []
    try:
        with txn(c):
            now = time.time()
            for r in c.execute("SELECT * FROM jobs WHERE proc=? AND state IN ('queued','active')", (PROC_ID,)).fetchall():
                end_job(r, "released" if r["state"] == "active" else "cancelled", now, events, restore, c, "session-ended", "cancel")
            c.execute("DELETE FROM processes WHERE id=?", (PROC_ID,))
    finally:
        c.close()
    flush(events, restore)


# ---------------------------------------------------------------- status / studio down

def status(args):
    c = db()
    try:
        cleanup(c)
        now = time.time()
        act = sorted(active_rows(c, now), key=lambda r: r["started"] or 0)
        q = queued_rows(c)
        f = down_flag(c)
        lines = [f"Studio: DOWN, reported by {f['agent']} at {time.strftime('%H:%M', time.localtime(f['at']))}: {f['reason']}" if f else "Studio: up"]
        lines.append(f"ACTIVE ({len(act)}): agent | lane | scope | purpose | age | expected")
        for r in act:
            exp = f"{r['minutes']:g}m" if r["minutes"] else "?"
            lines.append(f"  {r['agent']} | {lane_of(r)} | {fmt_scope(r)} | {one_line(r['purpose'], 50) or '-'} | {fmt_dur(now - (r['started'] or now))} | {exp}")
        lines.append(f"QUEUED ({len(q)}): # | agent | lane | scope | purpose | waited")
        for i, r in enumerate(q):
            away = "" if is_live(r, now) else " (away)"
            lines.append(f"  {i + 1} | {r['agent']} | {lane_of(r)} | {fmt_scope(r)} | {one_line(r['purpose'], 50) or '-'} | {fmt_dur(now - (r['queued_at'] or r['created']))}{away}")
        jid = args.get("job_id")
        if isinstance(jid, str) and jid:
            r = c.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
            if not r:
                lines.append(f"Your job {jid}: not found")
            elif r["state"] == "queued":
                pos, total = position(c, r)
                lines.append(f"Your job {jid}: position {pos} of {total}, ETA {fmt_eta(c, r, now)}")
            elif r["state"] == "active":
                lines.append(f"Your job {jid}: active, lease ends in {fmt_dur(r['lease_until'] - now)}")
            else:
                lines.append(f"Your job {jid}: {r['state']}")
        return {"text": "\n".join(lines)}
    finally:
        c.close()


def report_down(args):
    agent = need_str(args, "agent", "report_down")
    reason = one_line(args.get("reason"), 300) or "no reason given"
    c = db()
    try:
        with txn(c):
            c.execute("INSERT INTO flags(name,agent,reason,at) VALUES('studio_down',?,?,?) "
                      "ON CONFLICT(name) DO UPDATE SET agent=excluded.agent, reason=excluded.reason, at=excluded.at", (agent, reason, time.time()))
    finally:
        c.close()
    logging.warning("studio down by=%s reason=%s", agent, reason)
    wake_all()
    return {"text": "Studio marked down. Every acquire returns immediately until someone calls report_up. Stop and tell your orchestrator or user."}


def report_up(args):
    agent = need_str(args, "agent", "report_up")
    c = db()
    try:
        with txn(c):
            had = c.execute("DELETE FROM flags WHERE name='studio_down'").rowcount
    finally:
        c.close()
    logging.info("studio up by=%s was_down=%s", agent, bool(had))
    return {"text": "Studio marked up." if had else "Studio was not marked down."}


# ---------------------------------------------------------------- MCP plumbing

JOB = {"agent": {"type": "string", "description": "Your stable agent name."},
       "job_id": {"type": "string", "description": "Stable id for this request; reuse it for every call about it."}}
TOOLS = [
 {"name": "acquire", "description": "Get a lease before any blocking Studio action: Play mode, screenshots, camera/input (lane play, default, exclusive); MCP edits to instances (lane edit, concurrent with other edits unless scopes overlap); or a short screenshot/viewport change during edit work (lane camera, max 120 s). Reads (search, inspect, script reads) and filesystem script edits need no lease. Unless the user asks to bypass the queue, tell the user you are requesting a queue position first. Waits up to ~4.5 min; if it says 'still queued', call again at once with the same agent and job_id. Lease is 300 s; renew, release when done.",
  "inputSchema": {"type": "object", "properties": {**JOB,
     "lane": {"type": "string", "enum": list(LANES), "description": "play (default), edit or camera."},
     "scope": {"type": "array", "items": {"type": "string"}, "description": "edit lane: dotted instance paths you will change, e.g. [\"Workspace.Map\",\"StarterGui.Building\"]. Empty means the whole place."},
     "purpose": {"type": "string", "description": "Short reason, shown to others."},
     "minutes": {"type": "number", "description": "Expected length in minutes."}}, "required": ["agent", "job_id"]}},
 {"name": "release", "description": "Release your lease as soon as the work ends, including failures. Put Studio in a clean state first (Play stopped, debug pause off, camera restored) and say what you left in notes. Between test runs, release instead of holding; rejoin_seconds keeps your place for that long.",
  "inputSchema": {"type": "object", "properties": {**JOB,
     "notes": {"type": "string", "description": "State you left Studio in, for the next holder."},
     "rejoin_seconds": {"type": "number", "description": "0-300. If you acquire again within this time you go to the front. Reserves nothing meanwhile."}}, "required": ["agent", "job_id"]}},
 {"name": "renew", "description": "Extend your play or edit lease by 300 s before it expires. May tell you to release at the next safe point because a play request is waiting. Camera leases cannot be renewed.",
  "inputSchema": {"type": "object", "properties": JOB, "required": ["agent", "job_id"]}},
 {"name": "cancel", "description": "Drop your queued request, or release it if active. Use when you no longer need it (plan changed, giving up).",
  "inputSchema": {"type": "object", "properties": JOB, "required": ["agent", "job_id"]}},
 {"name": "status", "description": "Show who holds Studio (lane, scope, purpose, age), the queue, and an ETA for your job_id if given. No lease needed.",
  "inputSchema": {"type": "object", "properties": {"job_id": JOB["job_id"]}}},
 {"name": "report_down", "description": "Call when Studio or its MCP is disconnected or unusable. Makes every acquire return at once telling agents to stop. Do not keep retrying.",
  "inputSchema": {"type": "object", "properties": {"agent": JOB["agent"], "reason": {"type": "string"}}, "required": ["agent", "reason"]}},
 {"name": "report_up", "description": "Clear the Studio-down flag once Studio is working again.",
  "inputSchema": {"type": "object", "properties": {"agent": JOB["agent"]}, "required": ["agent"]}},
]
HANDLERS = {"release": release, "renew": renew, "cancel": cancel, "status": status,
            "report_down": report_down, "report_up": report_up}

INSTRUCTIONS = (
    "Unless the user explicitly asks to bypass or skip the queue, immediately before calling acquire clearly tell the user "
    "that you are requesting a position in the Roblox Studio playtest queue. "
    "Lanes: play (default, exclusive) for Play mode, screenshots, camera and input; edit for MCP edits to edit-time instances, "
    "pass scope = the dotted paths you change (edits with disjoint scopes run together); camera (max 120 s) for a screenshot "
    "or viewport change during edit work. Reads (search, inspect, script reads) and filesystem script edits need no lease. "
    "Give purpose and minutes. Use the same agent and job_id for acquire, renew, release and cancel. "
    "If acquire says 'still queued' it is not an error: call it again at once with the same values. "
    "Do not hold a lease while coding between runs: release (rejoin_seconds keeps your place) and acquire again. "
    "Release as soon as done and put Studio in a clean state. If Studio is down, call report_down and stop.")


def reply(i, result=None, error=None):
    out = {"jsonrpc": "2.0", "id": i, "result": result} if error is None else {"jsonrpc": "2.0", "id": i, "error": {"code": -32000, "message": str(error)}}
    with reply_lock:
        sys.stdout.write(json.dumps(out, separators=(",", ":")) + "\n"); sys.stdout.flush()


def read_message(stream):
    """Read a newline-delimited MCP JSON message; skips blank or malformed lines."""
    while True:
        line = stream.readline()
        if not line:
            return None
        try:
            return json.loads(line)
        except ValueError:
            continue


def is_acquire_call(msg):
    return (msg.get("method") == "tools/call" and isinstance(msg.get("params"), dict)
            and msg["params"].get("name") == "acquire" and msg.get("id") is not None)


def register_inflight(msg):
    """Register an acquire call's context before it runs, so a cancel notification can find it."""
    if not is_acquire_call(msg): return None
    ctx = INFLIGHT.get(msg["id"])
    if ctx is None:
        ctx = INFLIGHT[msg["id"]] = Ctx()
        a = msg["params"].get("arguments") or {}
        ctx.agent, ctx.job_id = a.get("agent"), a.get("job_id")
    return ctx


def handle(msg):
    method = msg.get("method"); i = msg.get("id"); ctx = None
    try:
        if method == "initialize": reply(i, {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "roblox-playtest-queue", "version": VERSION}, "instructions": INSTRUCTIONS})
        elif method == "notifications/initialized": pass
        elif method == "notifications/cancelled":
            ctx = INFLIGHT.get((msg.get("params") or {}).get("requestId"))
            if ctx:
                ctx.silent = True; ctx.cancelled.set(); ctx.wake.set()
                if isinstance(ctx.agent, str) and isinstance(ctx.job_id, str):
                    cancel({"agent": ctx.agent, "job_id": ctx.job_id})
            ctx = None
        elif method == "tools/list": reply(i, {"tools": TOOLS})
        elif method == "tools/call":
            name = msg["params"]["name"]; args = msg["params"].get("arguments") or {}
            if name == "acquire":
                ctx = register_inflight(msg) or Ctx()
                value = acquire(args, ctx)
            elif name in HANDLERS: value = HANDLERS[name](args)
            else: raise ValueError("unknown tool")
            if ctx is not None and ctx.silent: return  # the client cancelled this request; it is not listening
            reply(i, {"content": [{"type": "text", "text": value["text"]}]})
        elif i is not None: reply(i, {})
    except Exception as e:
        logging.exception("request failed method=%s", method)
        if i is not None and not (ctx is not None and ctx.silent):
            if method == "tools/call": reply(i, {"isError": True, "content": [{"type": "text", "text": f"Playtest queue error: {e}"}]})
            else: reply(i, error=e)
    finally:
        if method == "tools/call" and i is not None: INFLIGHT.pop(i, None)


def main():
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor(max_workers=8)
    try:
        ensure_process()
    except Exception:
        logging.exception("process registration failed")
    while True:
        msg = read_message(sys.stdin.buffer)
        if msg is None: break
        if is_acquire_call(msg):  # blocking calls get their own thread so release/cancel never queue behind them
            register_inflight(msg)
            threading.Thread(target=handle, args=(msg,), daemon=True).start()
        else:
            pool.submit(handle, msg)
    try:
        shutdown_process()
    except Exception:
        logging.exception("shutdown cleanup failed")
    logging.shutdown(); sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__": main()
