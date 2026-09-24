"""Environment-based configuration for the gateway."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .env import load_local_env


def _string_list(value: Any, *, field_name: str, server_name: str, path: Path) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SystemExit(f"MCP server {server_name!r} in {path} must define {field_name!r} as a string list.")
    return value


def _named_values(value: Any, *, field_name: str, server_name: str, path: Path) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, dict):
        entries = [{"name": name, "value": item} for name, item in value.items()]
    elif isinstance(value, list):
        entries = value
    else:
        raise SystemExit(f"MCP server {server_name!r} in {path} must define {field_name!r} as an object or list.")

    if not all(
        isinstance(entry, dict)
        and isinstance(entry.get("name"), str)
        and isinstance(entry.get("value"), str)
        for entry in entries
    ):
        raise SystemExit(
            f"MCP server {server_name!r} in {path} contains invalid {field_name!r} entries."
        )
    return [{"name": entry["name"], "value": entry["value"]} for entry in entries]


def load_mcp_servers(path: Path) -> list[dict[str, Any]]:
    """Load and normalize MCP config into ACP session/new server entries."""
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"Unable to read MCP config {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in MCP config {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise SystemExit(f"MCP config {path} must contain a JSON object.")

    servers = payload.get("mcpServers", {})
    if not isinstance(servers, dict):
        raise SystemExit(f"MCP config {path} must define 'mcpServers' as an object.")

    normalized: list[dict[str, Any]] = []
    for name, value in servers.items():
        if not isinstance(name, str) or not isinstance(value, dict):
            raise SystemExit(f"MCP config {path} contains an invalid server definition: {name!r}")

        server_type = value.get("type")
        if server_type in (None, "stdio", "local"):
            command = value.get("command")
            if not isinstance(command, str) or not command:
                raise SystemExit(f"stdio MCP server {name!r} in {path} must define a command.")
            normalized.append({
                "name": name,
                "command": command,
                "args": _string_list(value.get("args"), field_name="args", server_name=name, path=path),
                "env": _named_values(value.get("env"), field_name="env", server_name=name, path=path),
            })
        elif server_type in ("http", "sse"):
            url = value.get("url")
            if not isinstance(url, str) or not url:
                raise SystemExit(f"{server_type} MCP server {name!r} in {path} must define a URL.")
            normalized.append({
                "type": server_type,
                "name": name,
                "url": url,
                "headers": _named_values(
                    value.get("headers"),
                    field_name="headers",
                    server_name=name,
                    path=path,
                ),
            })
        else:
            raise SystemExit(f"MCP server {name!r} in {path} has unsupported type {server_type!r}.")
    return normalized


@dataclass(frozen=True)
class Config:
    slack_bot_token: str
    slack_app_token: str
    copilot_bin: str = "copilot"
    model: str | None = None
    mcp_servers: list[dict[str, Any]] = field(default_factory=list)
    cwd: str = ""
    approval_mode: str = "buttons"  # "buttons" (Slack approve/deny) or "auto" (--allow-all)
    allowed_users: frozenset[str] = field(default_factory=frozenset)
    state_dir: Path = Path.home() / ".copilot-slack-gateway"
    max_session_age_hours: float = 18.0
    show_thoughts: bool = False
    permission_timeout_seconds: float = 300.0
    prompt_timeout_seconds: float = 3600.0
    slack_workspace_url: str = "https://wesdigital.slack.com"

    @property
    def copilot_args(self) -> tuple[str, ...]:
        args = ["--acp", "--stdio"]
        if self.approval_mode == "auto":
            # --yolo == --allow-all-tools --allow-all-paths --allow-all-urls
            args.append("--yolo")
        return tuple(args)

    @classmethod
    def from_env(cls) -> "Config":
        load_local_env()

        bot = os.environ.get("SLACK_BOT_TOKEN", "").strip()
        app = os.environ.get("SLACK_APP_TOKEN", "").strip()
        if not bot or not app:
            raise SystemExit(
                "SLACK_BOT_TOKEN (xoxb-...) and SLACK_APP_TOKEN (xapp-...) are required.\n"
                "Create the Slack app from slack-app-manifest.yaml, enable Socket Mode, "
                "and put the tokens in .env (see .env.example)."
            )
        approval_mode = os.environ.get("APPROVAL_MODE", "buttons").strip().lower()
        if approval_mode not in ("buttons", "auto"):
            raise SystemExit(f"APPROVAL_MODE must be 'buttons' or 'auto', got {approval_mode!r}")

        mcp_config_path = Path(
            os.environ.get("COPILOT_MCP_CONFIG", str(Path.home() / ".copilot/mcp-config.json"))
        ).expanduser()

        return cls(
            slack_bot_token=bot,
            slack_app_token=app,
            slack_workspace_url=(
                os.environ.get("SLACK_WORKSPACE_URL", "https://wesdigital.slack.com").strip().rstrip("/")
                or "https://wesdigital.slack.com"
            ),
            copilot_bin=os.environ.get("COPILOT_BIN", "copilot").strip() or "copilot",
            model=(os.environ.get("COPILOT_MODEL") or os.environ.get("COPILOT_DEFAULT_MODEL") or "").strip() or None,
            mcp_servers=load_mcp_servers(mcp_config_path),
            cwd=str(Path(os.environ.get("GATEWAY_CWD", str(Path.home()))).expanduser().resolve()),
            approval_mode=approval_mode,
            allowed_users=frozenset(
                u.strip() for u in os.environ.get("ALLOWED_USERS", "").split(",") if u.strip()
            ),
            state_dir=Path(os.environ.get("GATEWAY_STATE_DIR", str(Path.home() / ".copilot-slack-gateway"))).expanduser(),
            max_session_age_hours=float(os.environ.get("MAX_SESSION_AGE_HOURS", "18")),
            show_thoughts=os.environ.get("SHOW_THOUGHTS", "").strip().lower() in ("1", "true", "yes"),
            permission_timeout_seconds=float(os.environ.get("PERMISSION_TIMEOUT_SECONDS", "300")),
            prompt_timeout_seconds=float(os.environ.get("PROMPT_TIMEOUT_SECONDS", "3600")),
        )
