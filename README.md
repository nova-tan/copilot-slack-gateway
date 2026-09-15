# copilot-slack-gateway

Control your **local** GitHub Copilot CLI from Slack. A small Python daemon
that connects to Slack over Socket Mode (outbound WebSocket, no public URL)
and drives a persistent `copilot --acp --stdio` process via the Agent Client
Protocol.

```
Slack (DM or @mention thread)
      │  Socket Mode (outbound)
      ▼
copilot-slack-gateway  ── spawns/owns ──▶  copilot --acp --stdio
      │  JSON-RPC over stdio (ACP)         one process + one ACP session
      ▼                                    per Slack thread, reused all day
streamed replies + permission buttons ◀── session/update, request_permission
```

## Lifecycle

- **Persistent**: each Slack DM/thread maps to one Copilot session. Follow-up
  messages reuse it (multi-turn memory verified by `scripts/acp_smoke.py`).
- **Daily rollover**: sessions older than `MAX_SESSION_AGE_HOURS` (default 18)
  are recycled on the next message.
- **Crash recovery**: if the Copilot process exits, the gateway tells the
  thread and starts a fresh session on the next message.
- **Gateway restarts**: Copilot child processes die with the gateway; threads
  transparently get a fresh session on their next message.

## Setup

1. **Create the Slack app** at <https://api.slack.com/apps> → *Create New App* →
   *From a manifest*. First copy the public example to the ignored local
   manifest, then paste it:

   ```bash
   cp slack-app-manifest.example.yaml slack-app-manifest.yaml
   ```
2. **Enable Socket Mode**: *Settings → Socket Mode* → enable, generate an
   app-level token (`xapp-...`) with `connections:write`.
3. **Install to workspace** and copy the bot token (`xoxb-...`).
4. Configure:

   ```bash
   cp .env.example .env   # fill in tokens and ALLOWED_USERS
   python3 -m venv .venv && .venv/bin/pip install -e .
   ```

5. Run:

   ```bash
   .venv/bin/copilot-slack-gateway        # or: python -m copilot_gateway
   ```

6. **Keepalive (optional)**: see
   `launchd/com.example.copilot-slack-gateway.plist`. Replace the
   `__PROJECT_DIR__` and `__HOME_DIR__` placeholders before loading it.

The checked-in example manifest registers the built-in gateway commands. The
local `slack-app-manifest.yaml` is intentionally ignored because its slash
commands may be workspace-specific. To register additional Copilot skills as
slash commands, edit the local manifest directly or ask your coding agent to
update it.

## Usage

DM the bot (or @mention it in a channel — replies stay in the thread):

- just type — talk to Copilot
- `/new` — discard this thread's session, start fresh
- `/stop` — cancel the running prompt
- `/tasks` — active subagents and shell commands in this Copilot session
- `/help` — list commands

Tool permission requests appear as **Approve / Deny buttons** in the thread
(`APPROVAL_MODE=buttons`, the default). Set `APPROVAL_MODE=auto` to run
Copilot with `--allow-all-*` flags instead (yolo).

## Configuration

See `.env.example`. Notables:

| Var | Default | Purpose |
|---|---|---|
| `ALLOWED_USERS` | _(empty)_ | Comma-separated Slack user IDs allowed to use the bot. Empty denies all users. **Set this.** |
| `GATEWAY_CWD` | `~` | Working dir for all Copilot sessions; ACP fs bridge is confined here |
| `COPILOT_MODEL` | CLI default | Optional model ID supported by your Copilot CLI |
| `MAX_SESSION_AGE_HOURS` | `18` | "Daily rollover" threshold |
| `COPILOT_RELAX_X509_STRICT` | `false` | Opt-in compatibility for enterprise TLS proxies with legacy certificate chains |

## Testing

```bash
.venv/bin/python scripts/acp_smoke.py   # ACP only, no Slack tokens needed
```

## Limitations

- One prompt at a time per thread; messages sent while busy are queued FIFO.
- Session history lives in the Copilot process; a gateway restart starts a
  fresh Copilot session (Slack history remains for humans).
- File access via the ACP fs bridge is confined to `GATEWAY_CWD`.
