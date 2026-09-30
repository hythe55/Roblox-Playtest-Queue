#!/usr/bin/env python3
"""Manual terminal client for the Roblox playtest queue.

Usage:
  queue_cli.py acquire [--lane play|edit|camera] [--scope A.B,C.D] [--purpose TEXT] [--minutes N]
  queue_cli.py release [--notes TEXT] [--rejoin SECONDS]
  queue_cli.py renew | cancel | status
  queue_cli.py report-down REASON... | report-up
"""
import argparse
import getpass
import json
import os
import sys
import uuid

import server

server.set_exempt_process()  # manual leases must outlive this process; they end by release or lease expiry

STATE_FILE = os.environ.get("ROBLOX_PLAYTEST_CLI_STATE") or os.path.join(
    os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
    "RobloxPlaytestQueue",
    "manual-lease.json",
)
AGENT = f"manual:{getpass.getuser()}"


def notify_acquired():
    print("Queue acquired - Roblox Studio is yours.", flush=True)
    if os.name == "nt":
        try:
            import winsound
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
        except Exception:
            pass


def read_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as stream:
            return json.load(stream)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_state(job_id):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as stream:
        json.dump({"job_id": job_id, "agent": AGENT}, stream)


def clear_state():
    try:
        os.remove(STATE_FILE)
    except FileNotFoundError:
        pass


def acquire(args):
    current = read_state()
    if current:
        print(f"Already holding queue lease job_id={current['job_id']}")
        return 0
    job_id = str(uuid.uuid4())
    req = {"job_id": job_id, "agent": AGENT, "lane": args.lane, "purpose": args.purpose or "manual",
           "minutes": args.minutes, "scope": [s for s in (args.scope or "").split(",") if s.strip()]}
    print(f"Waiting for the Roblox Studio queue (job_id={job_id})...", flush=True)
    try:
        while True:
            result = server.acquire(req)
            if result["status"] == "queued":
                print(result["text"].split(" Call acquire")[0], flush=True)
                continue
            break
    except KeyboardInterrupt:
        server.cancel({"job_id": job_id, "agent": AGENT})
        print("Cancelled.", file=sys.stderr)
        return 1
    if result["status"] != "granted":
        print(result["text"], file=sys.stderr)
        return 1
    write_state(job_id)
    notify_acquired()
    print(f"Lease expires at {result['expires_at']:.0f}. Run 'renew' to extend, 'release' when finished.", flush=True)
    return 0


def with_state(fn, done):
    current = read_state()
    if not current:
        print("No manual queue lease is recorded.", file=sys.stderr)
        return 1
    value = fn(current)
    print(done(current, value))
    return 0


def release(args):
    def do(cur):
        value = server.release({"job_id": cur["job_id"], "agent": AGENT, "notes": args.notes, "rejoin_seconds": args.rejoin})
        clear_state()
        return value
    return with_state(do, lambda cur, v: f"Queue released. job_id={cur['job_id']}")


def renew(args):
    return with_state(lambda cur: server.renew({"job_id": cur["job_id"], "agent": AGENT}), lambda cur, v: v["text"])


def cancel(args):
    def do(cur):
        value = server.cancel({"job_id": cur["job_id"], "agent": AGENT})
        clear_state()
        return value
    return with_state(do, lambda cur, v: v["text"])


def status(args):
    current = read_state()
    print(server.status({"job_id": current["job_id"] if current else None})["text"])
    return 0


def report_down(args):
    print(server.report_down({"agent": AGENT, "reason": " ".join(args.reason)})["text"])
    return 0


def report_up(args):
    print(server.report_up({"agent": AGENT})["text"])
    return 0


def main():
    p = argparse.ArgumentParser(description="Manual client for the Roblox Studio queue")
    sub = p.add_subparsers(dest="command", required=True)
    a = sub.add_parser("acquire")
    a.add_argument("--lane", choices=server.LANES, default="play")
    a.add_argument("--scope", help="comma-separated dotted instance paths (edit lane)")
    a.add_argument("--purpose")
    a.add_argument("--minutes", type=float)
    a.set_defaults(fn=acquire)
    r = sub.add_parser("release")
    r.add_argument("--notes")
    r.add_argument("--rejoin", type=float, default=0, help="seconds (0-300) to keep your place")
    r.set_defaults(fn=release)
    for name, fn in (("renew", renew), ("cancel", cancel), ("status", status), ("report-up", report_up)):
        sub.add_parser(name).set_defaults(fn=fn)
    d = sub.add_parser("report-down")
    d.add_argument("reason", nargs="+")
    d.set_defaults(fn=report_down)
    args = p.parse_args()
    try:
        return args.fn(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
