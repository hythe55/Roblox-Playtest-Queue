#!/usr/bin/env python3
"""Small stdio MCP server that serializes Roblox Studio playtest leases."""
import json, logging, os, sqlite3, sys, threading, time, uuid
from audio import mute_studio, restore_studio

DEFAULT_DB_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "RobloxPlaytestQueue")
DB = os.environ.get("ROBLOX_PLAYTEST_QUEUE_DB", os.path.join(DEFAULT_DB_DIR, "queue.db"))
LEASE_SECONDS = int(os.environ.get("ROBLOX_PLAYTEST_LEASE_SECONDS", "900"))
QUEUE_WAIT_SECONDS = int(os.environ.get("ROBLOX_PLAYTEST_QUEUE_WAIT_SECONDS", "3600"))
LOG_FILE = os.environ.get("ROBLOX_PLAYTEST_QUEUE_LOG", os.path.join(os.path.dirname(__file__), "queue.log"))
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
reply_lock = threading.Lock()

def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, agent TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL, started REAL, lease_until REAL, released REAL, UNIQUE(id))")
    c.commit(); return c

def cleanup(c):
    now = time.time()
    expired = c.execute("SELECT id FROM jobs WHERE state='active' AND lease_until < ?", (now,)).fetchall()
    if expired:
        for row in expired: restore_studio(row["id"])
    c.execute("UPDATE jobs SET state='expired', released=? WHERE state='active' AND lease_until < ?", (now, now))
    c.execute("UPDATE jobs SET state='expired', released=? WHERE state='queued' AND created < ?", (now, now - QUEUE_WAIT_SECONDS))
    c.commit()

def acquire(args):
    job_id = args.get("job_id")
    agent = args.get("agent")
    if not isinstance(job_id, str) or not job_id: raise ValueError("acquire requires a stable job_id")
    if not isinstance(agent, str) or not agent: raise ValueError("acquire requires agent")
    c = db(); c.execute("INSERT OR IGNORE INTO jobs(id,agent,state,created) VALUES(?,?, 'queued', ?)", (job_id, agent, time.time())); c.commit()
    while True:
        cleanup(c)
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row: raise ValueError("job not found")
        if row["state"] == "active":
            return {"job_id": job_id, "lease_id": job_id, "expires_at": row["lease_until"], "lease_seconds": LEASE_SECONDS}
        if row["state"] in ("released", "expired"):
            raise ValueError(f"job {job_id} is already {row['state']}; use a new job_id")
        c.execute("BEGIN IMMEDIATE")
        try:
            now = time.time()
            active = c.execute("SELECT 1 FROM jobs WHERE state='active' AND lease_until >= ? LIMIT 1", (now,)).fetchone()
            ahead = c.execute("SELECT 1 FROM jobs WHERE state='queued' AND created < ? LIMIT 1", (row["created"],)).fetchone()
            if not active and not ahead:
                until = now + LEASE_SECONDS
                changed = c.execute("UPDATE jobs SET state='active',started=?,lease_until=? WHERE id=? AND state='queued'", (now, until, job_id)).rowcount
                if not changed:
                    c.rollback()
                    continue
                c.commit()
                mute_studio(job_id)
                logging.info("acquire job=%s agent=%s", job_id, agent)
                continue
            c.rollback()
        except Exception:
            c.rollback()
            raise
        time.sleep(2)

def release(args):
    if not isinstance(args.get("job_id"), str) or not args["job_id"]:
        raise ValueError("release requires a non-empty job_id")
    agent = args.get("agent")
    if not isinstance(agent, str) or not agent: raise ValueError("release requires agent")
    c = db(); now = time.time(); cur = c.execute("UPDATE jobs SET state='released',released=? WHERE id=? AND state='active' AND agent=?", (now, args.get("job_id"), agent)); c.commit()
    if not cur.rowcount:
        raise ValueError("job_id is not an active lease owned by this queue client")
    restore_studio(args["job_id"]); logging.info("release job=%s agent=%s", args["job_id"], agent)
    return {"released": bool(cur.rowcount), "job_id": args.get("job_id")}

def renew(args):
    if not isinstance(args.get("job_id"), str) or not args["job_id"]:
        raise ValueError("renew requires a non-empty job_id")
    agent = args.get("agent")
    if not isinstance(agent, str) or not agent: raise ValueError("renew requires agent")
    c = db(); now = time.time(); until = now + LEASE_SECONDS
    cur = c.execute("UPDATE jobs SET lease_until=? WHERE id=? AND state='active' AND lease_until >= ? AND agent=?", (until, args.get("job_id"), now, agent)); c.commit()
    if not cur.rowcount: raise ValueError("lease is missing or expired")
    return {"job_id": args.get("job_id"), "expires_at": until, "lease_seconds": LEASE_SECONDS}

TOOLS = [
 {"name":"acquire","description":"Before calling this tool, clearly tell the user that you are requesting a position in the Roblox Studio playtest queue. Then acquire the exclusive Roblox Studio playtest lease. Call immediately before any Roblox Studio playtest. This waits internally in FIFO order; do not report queue details while waiting. When it returns, proceed normally. Always provide a stable unique job_id and agent name; retry with the same values after a client timeout. Lease lasts 900 seconds.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"},"job_id":{"type":"string"}},"required":["agent","job_id"]}},
 {"name":"release","description":"Release your own lease immediately after your playtest ends, including failures. Provide the same agent and job_id used by acquire.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"},"job_id":{"type":"string"}},"required":["agent","job_id"]}},
 {"name":"renew","description":"Renew your active lease before expiry. Provide the same agent and job_id used by acquire.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"},"job_id":{"type":"string"}},"required":["agent","job_id"]}}
]

def reply(i, result=None, error=None):
    out = {"jsonrpc":"2.0","id":i,"result":result} if error is None else {"jsonrpc":"2.0","id":i,"error":{"code":-32000,"message":str(error)}}
    with reply_lock:
        sys.stdout.write(json.dumps(out, separators=(",", ":")) + "\n"); sys.stdout.flush()

def read_message(stream):
    """Read a newline-delimited MCP JSON message."""
    line = stream.readline()
    if not line:
        return None
    return json.loads(line)

def handle(msg):
    method=msg.get("method"); i=msg.get("id")
    try:
        if method == "initialize": reply(i,{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"roblox-playtest-queue","version":"0.2.0"},"instructions":"Immediately before calling acquire, clearly tell the user that you are requesting a position in the Roblox Studio playtest queue. Call acquire before any Roblox Studio playtest. It waits internally and returns only when ready; do not report queue details while waiting. Use the same agent and job_id for release."})
        elif method == "notifications/initialized": pass
        elif method == "tools/list": reply(i,{"tools":TOOLS})
        elif method == "tools/call":
            name=msg["params"]["name"]; args=msg["params"].get("arguments",{})
            value = acquire(args) if name=="acquire" else release(args) if name=="release" else renew(args) if name=="renew" else (_ for _ in ()).throw(ValueError("unknown tool"))
            text = f"Ready. Use the same agent and job_id={value['job_id']} when releasing or renewing." if name=="acquire" else f"Playtest lease released. job_id={value['job_id']}" if name=="release" else f"Playtest lease renewed. job_id={value['job_id']} expires_at={value['expires_at']}"
            reply(i,{"content":[{"type":"text","text":text}]})
        elif i is not None: reply(i,{})
    except Exception as e:
        logging.exception("request failed method=%s", method)
        if i is not None:
            if method == "tools/call": reply(i,{"isError":True,"content":[{"type":"text","text":f"Playtest queue error: {e}"}]})
            else: reply(i,error=e)

def main():
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor(max_workers=16)
    while True:
        msg = read_message(sys.stdin.buffer)
        if msg is None: break
        pool.submit(handle, msg)
if __name__ == "__main__": main()
