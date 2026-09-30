# Roblox Playtest Queue

An external MCP server (version 0.3.0) that shares one Roblox Studio between several agents. It does not modify or depend on the Roblox Studio MCP.

Every Claude Code session spawns its own `server.py`; all of them share one sqlite database (`%LOCALAPPDATA%\RobloxPlaytestQueue\queue.db`). Subagents of one session share that session's process.

## Run

```powershell
python server.py
```

On Windows, set `ROBLOX_PLAYTEST_MUTE_AUDIO=true` to mute Roblox Studio's application audio while a play lease is active and restore each session's prior mute state on release. Install the optional audio dependencies with `python -m venv .venv` and `.venv\Scripts\pip install -r requirements.txt`.

## Lanes

| Lane | Use for | Concurrency |
| --- | --- | --- |
| `play` (default) | Play mode, screenshots, camera, input | Exclusive against everything |
| `edit` | MCP edits to edit-time instances | Runs alongside other edits unless their `scope` overlaps |
| `camera` | A screenshot or viewport change during edit work | One at a time; max 120 s; not renewable; compatible with edit, blocked by play |

Reads (search, inspect, script reads) and filesystem script edits need no lease.

An `edit` lease takes `scope`, a list of dotted instance paths such as `["Workspace.Map", "StarterGui.Building"]`. Two scopes conflict when one equals the other or is its ancestor at a dot boundary (`Workspace.Map` conflicts with `Workspace.Map.Tower`, not with `Workspace.MapExtras`). A missing or empty scope means the whole place and conflicts with every edit. A leading `game.` is dropped (`game.Workspace.Map` is `Workspace.Map`), `game` alone means the whole place, and a scope given as one string is split on commas. Rows from old clients (NULL lane) count as play.

### Ordering

Grants follow queue time. A job is granted when no active lease conflicts with it and no live, earlier-queued job conflicts with it. So a play waits for earlier edits and blocks later ones, while non-overlapping edits may pass each other. A waiter counts as live while its `acquire` call is polling, and for `ROBLOX_PLAYTEST_LIVE_SECONDS` after; a waiter that is between calls longer than that keeps its place but does not hold others up.

While a play request is waiting, `renew` on an edit lease still succeeds but tells the holder to release at the next safe point. Once that play has waited longer than one lease length, edit renews are refused.

### Bounded waits

`acquire` blocks at most `ROBLOX_PLAYTEST_ACQUIRE_MAX_WAIT_SECONDS` (default 270, chosen so a re-call stays under the 5-minute prompt cache). If it is not granted by then it returns a normal result ("still queued", position, ETA) telling the agent to call again with the same `agent` and `job_id`. The job keeps its place for `ROBLOX_PLAYTEST_GRACE_SECONDS` (default 120); after that it becomes `abandoned`. A job that is still being polled is never expired for age; `ROBLOX_PLAYTEST_QUEUE_WAIT_SECONDS` applies only to rows from old-code processes, which have no `last_seen`. A job is only ever granted by its own polling thread, so a lease can never go to a request nobody is waiting on.

### Other rules

- `agent` must be unique per agent; two agents sharing a name supersede each other's queued requests.
- A `job_id` belongs to the agent that first used it; `acquire` with another agent's `job_id` is an error, so two agents can never share one lease.
- One request per agent: a new `acquire` with a different `job_id` supersedes the agent's older queued job. A lease the agent already holds is not touched (the response says so).
- `cancel` drops a queued job or releases an active one. The server also handles MCP `notifications/cancelled` and cancels the matching in-flight `acquire`.
- Each process heartbeats every `ROBLOX_PLAYTEST_HEARTBEAT_SECONDS`. Jobs of a process not seen for `ROBLOX_PLAYTEST_PROCESS_TIMEOUT_SECONDS` are expired (audio restored for active ones). On stdin EOF a process releases and cancels its own jobs before exiting.
- `release` with `rejoin_seconds` (0-300) lets the same agent queue as of the moment it released if it acquires again within that window: ahead of requests made while it was away, never ahead of one that was already waiting. It reserves nothing meanwhile.
- `release` with `notes` records the state Studio was left in. A play grant shows the latest other agent's notes from the last 30 minutes.
- `report_down` sets a global flag; while set, every `acquire` (including waiting ones) returns at once telling the agent to stop. Active leases continue. `report_up` clears it.

## Tools

| Tool | Signature |
| --- | --- |
| `acquire` | `acquire(agent, job_id, lane?, scope?, purpose?, minutes?)` |
| `release` | `release(agent, job_id, notes?, rejoin_seconds?)` |
| `renew` | `renew(agent, job_id)` (+300 s; reports when over 150% of the estimate) |
| `cancel` | `cancel(agent, job_id)` |
| `status` | `status(job_id?)` compact table of active leases and the queue, ETA for `job_id` |
| `report_down` | `report_down(agent, reason)` |
| `report_up` | `report_up(agent)` |

Old calls `acquire(agent, job_id)` keep working (play lane). Use a stable `job_id`; re-call with the same values when `acquire` says it is still queued.

The ETA is the sum of the remaining expected minutes of the conflicting leases and waiters ahead; it is "unknown" when any of them gave no `minutes`.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROBLOX_PLAYTEST_QUEUE_DB` | `%LOCALAPPDATA%\RobloxPlaytestQueue\queue.db` | Shared database path |
| `ROBLOX_PLAYTEST_QUEUE_LOG` | `queue.log` next to `server.py` | Log file |
| `ROBLOX_PLAYTEST_LEASE_SECONDS` | 300 | Play and edit lease length, and renew extension |
| `ROBLOX_PLAYTEST_CAMERA_SECONDS` | 120 | Camera lease length (capped at 120) |
| `ROBLOX_PLAYTEST_ACQUIRE_MAX_WAIT_SECONDS` | 270 | Longest one `acquire` call blocks |
| `ROBLOX_PLAYTEST_GRACE_SECONDS` | 120 | How long a queued job keeps its place between `acquire` calls |
| `ROBLOX_PLAYTEST_LIVE_SECONDS` | 90 | A waiter unseen this long no longer holds up others |
| `ROBLOX_PLAYTEST_QUEUE_WAIT_SECONDS` | 3600 | Maximum age of a queued job from an old-code process (new-code jobs use the grace period instead) |
| `ROBLOX_PLAYTEST_POLL_SECONDS` | 2 | Polling interval of a waiting `acquire` |
| `ROBLOX_PLAYTEST_HEARTBEAT_SECONDS` | 10 | Process heartbeat interval |
| `ROBLOX_PLAYTEST_PROCESS_TIMEOUT_SECONDS` | 60 | A process unseen this long is dead |
| `ROBLOX_PLAYTEST_MUTE_AUDIO` | off | `true` mutes Studio audio during play leases |
| `ROBLOX_PLAYTEST_AUDIO_PROCESS` | `RobloxStudioBeta.exe` | Process whose audio is muted |
| `ROBLOX_PLAYTEST_AUDIO_POLL_SECONDS` | 0.5 | Mute watcher interval |
| `ROBLOX_PLAYTEST_AUDIO_STATE` | `audio-state.json` next to `audio.py` | Saved mute state |
| `ROBLOX_PLAYTEST_CLI_STATE` | `%LOCALAPPDATA%\RobloxPlaytestQueue\manual-lease.json` | `queue_cli.py` lease record |

## Log lines

The prefixes `INFO acquire job=... agent=...` and `INFO release job=... agent=...` are unchanged; ` lane=... scope=... purpose=...` is appended. Other events: `INFO expire|abandon|cancel|supersede job=... agent=... lane=... scope=... purpose=... was=... reason=...`, `WARNING studio down by=... reason=...` and `INFO studio up by=...`.

## Database

Schema changes are additive only (new columns via `ALTER TABLE ADD COLUMN` guarded by `PRAGMA table_info`, new tables `processes` and `flags`), so old and new processes can share one database. Old rows stay valid. Old processes treat every lease as exclusive, which is safe. Jobs owned by old processes (no `proc`) are never ended with states they do not know.

## Manual client

```powershell
python queue_cli.py acquire [--lane edit --scope Workspace.Map --purpose "..." --minutes 5]
python queue_cli.py status | renew | cancel
python queue_cli.py release [--notes "..."] [--rejoin 60]
python queue_cli.py report-down "Studio crashed"
python queue_cli.py report-up
```

A manual lease is exempt from dead-process cleanup; it ends by `release` or lease expiry.

## Tests

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests use a temporary database and log and never touch the live queue.

## Claude Code

Add the project-scoped server:

```bash
claude mcp add --scope project roblox-playtest-queue -- .venv\\Scripts\\python.exe server.py
```

## Codex

Configure the same stdio command in the MCP server settings or package it in a Codex plugin. The command is `python server.py` with this repository as its working directory.
