#!/usr/bin/env python3
"""Small stdio MCP server that serializes Roblox Studio playtest leases."""
import json, os, sqlite3, sys, time, uuid
from audio import mute_studio, restore_studio

DB = os.environ.get("ROBLOX_PLAYTEST_QUEUE_DB", os.path.join(os.path.dirname(__file__), "queue.db"))
LEASE_SECONDS = int(os.environ.get("ROBLOX_PLAYTEST_LEASE_SECONDS", "900"))

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
        restore_studio()
    c.execute("UPDATE jobs SET state='expired', released=? WHERE state='active' AND lease_until < ?", (now, now))
    c.commit()

def acquire(args):
    job_id = args.get("job_id") or str(uuid.uuid4())
    agent = args.get("agent") or "unknown"
    c = db(); c.execute("INSERT OR IGNORE INTO jobs(id,agent,state,created) VALUES(?,?, 'queued', ?)", (job_id, agent, time.time())); c.commit()
    while True:
        cleanup(c)
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row: raise ValueError("job not found")
        if row["state"] == "active":
            return {"job_id": job_id, "lease_id": job_id, "expires_at": row["lease_until"], "lease_seconds": LEASE_SECONDS}
        if row["state"] in ("released", "expired"):
            raise ValueError(f"job {job_id} is already {row['state']}; use a new job_id")
        active = c.execute("SELECT 1 FROM jobs WHERE state='active' AND lease_until >= ? LIMIT 1", (time.time(),)).fetchone()
        ahead = c.execute("SELECT 1 FROM jobs WHERE state='queued' AND created < ? LIMIT 1", (row["created"],)).fetchone()
        if not active and not ahead:
            now = time.time(); until = now + LEASE_SECONDS
            c.execute("UPDATE jobs SET state='active',started=?,lease_until=? WHERE id=? AND state='queued'", (now, until, job_id)); c.commit()
            mute_studio()
            continue
        time.sleep(2)

def release(args):
    if not isinstance(args.get("job_id"), str) or not args["job_id"]:
        raise ValueError("release requires a non-empty job_id")
    c = db(); now = time.time(); cur = c.execute("UPDATE jobs SET state='released',released=? WHERE id=? AND state='active'", (now, args.get("job_id"))); c.commit()
    if not cur.rowcount:
        raise ValueError("job_id is not an active lease owned by this queue client")
    restore_studio()
    return {"released": bool(cur.rowcount), "job_id": args.get("job_id")}

def renew(args):
    if not isinstance(args.get("job_id"), str) or not args["job_id"]:
        raise ValueError("renew requires a non-empty job_id")
    c = db(); now = time.time(); until = now + LEASE_SECONDS
    cur = c.execute("UPDATE jobs SET lease_until=? WHERE id=? AND state='active' AND lease_until >= ?", (until, args.get("job_id"), now)); c.commit()
    if not cur.rowcount: raise ValueError("lease is missing or expired")
    return {"job_id": args.get("job_id"), "expires_at": until, "lease_seconds": LEASE_SECONDS}

TOOLS = [
 {"name":"acquire","description":"Acquire the exclusive Roblox Studio playtest lease. Call this immediately before ANY Roblox Studio playtest or playtest command, including calls to another Roblox Studio MCP server. This call waits until your FIFO turn is available. Do NOT start, continue, or issue playtest commands while another agent owns the lease, and do NOT use another agent's active play session. Do not run the playtest until this tool succeeds and returns your own job_id. The lease lasts 900 seconds by default; use renew if needed. Retry with the same job_id after a client timeout.","inputSchema":{"type":"object","properties":{"agent":{"type":"string"},"job_id":{"type":"string"}},"required":[]}},
 {"name":"release","description":"Release your own Roblox Studio playtest lease immediately after the playtest ends, including when the test fails. Never release another agent's job_id. Releasing does not start a playtest; acquire must succeed first.","inputSchema":{"type":"object","properties":{"job_id":{"type":"string"}},"required":["job_id"]}},
 {"name":"renew","description":"Renew an active Roblox Studio playtest lease before it expires. Use the job_id returned by acquire.","inputSchema":{"type":"object","properties":{"job_id":{"type":"string"}},"required":["job_id"]}}
]

def reply(i, result=None, error=None):
    out = {"jsonrpc":"2.0","id":i,"result":result} if error is None else {"jsonrpc":"2.0","id":i,"error":{"code":-32000,"message":str(error)}}
    sys.stdout.write(json.dumps(out, separators=(",", ":")) + "\n"); sys.stdout.flush()

def read_message(stream):
    """Read newline-delimited MCP JSON, with compatibility for old framed clients."""
    line = stream.readline()
    if not line:
        return None
    if line.startswith(b"Content-Length:"):
        length = int(line.split(b":", 1)[1])
        stream.readline()
        return json.loads(stream.read(length))
    return json.loads(line)

def main():
    while True:
        msg = read_message(sys.stdin.buffer)
        if msg is None: break
        method=msg.get("method"); i=msg.get("id")
        try:
            if method == "initialize": reply(i,{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"roblox-playtest-queue","version":"0.1.0"},"instructions":"Before ANY Roblox Studio playtest command, call acquire and wait for it to succeed. Never start or continue a playtest using another agent's active session. If acquire is rejected or times out, stop: do not run playtest commands; retry the same job_id or explain the failure. Call release immediately when your own playtest ends."})
            elif method == "notifications/initialized": pass
            elif method == "tools/list": reply(i,{"tools":TOOLS})
            elif method == "tools/call":
                name=msg["params"]["name"]; args=msg["params"].get("arguments",{})
                value = acquire(args) if name=="acquire" else release(args) if name=="release" else renew(args) if name=="renew" else (_ for _ in ()).throw(ValueError("unknown tool"))
                reply(i,{"content":[{"type":"text","text":json.dumps(value)}],"structuredContent":value})
            elif i is not None: reply(i,{})
        except Exception as e:
            if i is not None:
                if method == "tools/call":
                    reply(i,{"isError":True,"content":[{"type":"text","text":f"Playtest queue error: {e}"}]})
                else:
                    reply(i,error=e)
if __name__ == "__main__": main()
