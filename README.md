# Roblox Playtest Queue

An external MCP server that serializes access to a single Roblox Studio playtest slot. It does not modify or depend on the Roblox Studio MCP.

## Run

```powershell
python server.py
```

The server exposes `acquire`, `release`, and `renew`. `acquire` waits in FIFO order and returns a lease that lasts 900 seconds by default. Set `ROBLOX_PLAYTEST_QUEUE_DB` for a shared database path and `ROBLOX_PLAYTEST_LEASE_SECONDS` to change the lease length.

Use a stable `job_id` when retrying an interrupted acquire so the same queue entry is resumed.

## Claude Code

Add the project-scoped server:

```bash
claude mcp add --scope project roblox-playtest-queue -- python server.py
```

## Codex

Configure the same stdio command in the MCP server settings or package it in a Codex plugin. The command is `python server.py` with this repository as its working directory.
