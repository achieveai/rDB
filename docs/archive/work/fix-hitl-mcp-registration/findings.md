# Fix: hitl MCP server registration (2026-09-17)

## Symptom
`mcp__hitl__*` tools unavailable in session. hitl not listed as configured or failed server.

## Where config actually lives
- `~/.claude/claude.json` — **does not exist**.
- `~/.claude/.claude.json` — 423-byte stub (firstStartTime, machineID, userID only). Not the MCP config.
- `~/.claude.json` — the real global config (51 KB). MCP servers go here under top-level `mcpServers`.
- `~/.claude/settings.json` — had `mcp__hitl__Notify` in permissions.allow, but no server definition (permissions ≠ registration).

## Root cause
`~/.claude.json.backup` (22:51) contained:

```json
"mcpServers": { "hitl": { "args": { "/c", "npx", ... } } }
```

`args` used **object braces `{}`** around a bare list → invalid JSON. Claude Code failed to parse
`~/.claude.json`, rewrote it, and the whole `mcpServers` key was dropped.

`~/.claude.json.swp` is a **Vim swap file** (binary, same 22:52 timestamp) → the corruption came from a
hand-edit in vim.

## Fix applied
Added to `~/.claude.json` (top level, after `orgModelDefaultCache`) with `args` as a JSON array:

```json
"mcpServers": {
  "hitl": {
    "type": "stdio",
    "command": "cmd",
    "args": ["/c", "npx", "-y", "@achieveai/hitl-mcp-server@latest", "--no-auto-launch-client"],
    "env": {},
    "timeout": 2147483647
  }
}
```

Also repaired the same `{` → `[` typo in `~/.claude.json.corrupted`-adjacent `~/.claude.json.backup`
so a future restore-from-backup doesn't reintroduce it.

## Verification
- `ConvertFrom-Json` on `~/.claude.json` → parses, `mcpServers.hitl` present.
- `ConvertFrom-Json` on `~/.claude.json.backup` → parses.
- Handshake smoke test:
  `echo '{"jsonrpc":"2.0","id":1,"method":"initialize",...}' | npx -y @achieveai/hitl-mcp-server@latest --no-auto-launch-client`
  → `hitl-mcp-server v2.13.1 running on stdio (ntfy-backed)` + valid initialize result.

## Notes
- Package: `@achieveai/hitl-mcp-server`, latest 2.13.1.
- Client config at `~/.hitl/config.json` (ntfy URL `https://local-gb.mcqdb.com/ntfy/`, device `devsFour`) is intact.
- `--no-auto-launch-client` means the notification client is NOT started by the MCP server; it must run separately.
- Requires a Claude Code restart to pick up the server.
- Backup of pre-fix `~/.claude.json` in this session's scratchpad: `claude.json.pre-hitl-fix`.
- `~/.claude.json.swp` left in place (stale vim swap — user's call to delete).
