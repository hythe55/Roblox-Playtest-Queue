#!/usr/bin/env python3
"""Manual terminal client for the Roblox playtest queue."""
import getpass
import json
import os
import sys
import uuid

import server

STATE_FILE = os.path.join(
    os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
    "RobloxPlaytestQueue",
    "manual-lease.json",
)
AGENT = f"manual:{getpass.getuser()}"


def notify_acquired():
    print("Queue acquired — Roblox Studio is yours.", flush=True)
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


def acquire():
    current = read_state()
    if current:
        print(f"Already holding queue lease job_id={current['job_id']}")
        return 0
    job_id = str(uuid.uuid4())
    print(f"Waiting for the Roblox Studio queue (job_id={job_id})...", flush=True)
    result = server.acquire({"job_id": job_id, "agent": AGENT})
    write_state(job_id)
    notify_acquired()
    print(f"Lease expires at {result['expires_at']:.0f}. Run 'release' when finished.", flush=True)
    return 0


def release():
    current = read_state()
    if not current:
        print("No manual queue lease is recorded.", file=sys.stderr)
        return 1
    server.release({"job_id": current["job_id"], "agent": AGENT})
    try:
        os.remove(STATE_FILE)
    except FileNotFoundError:
        pass
    print(f"Queue released. job_id={current['job_id']}")
    return 0


def main():
    command = sys.argv[1].lower() if len(sys.argv) > 1 else ""
    if command == "acquire":
        return acquire()
    if command == "release":
        return release()
    print("Usage: acquire | release", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
