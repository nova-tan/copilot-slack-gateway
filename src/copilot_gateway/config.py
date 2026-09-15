"""Environment-based configuration for the gateway."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from .env import load_local_env


@dataclass(frozen=True)
class Config:
    slack_bot_token: str
    slack_app_token: str
    copilot_bin: str = "copilot"
    model: str | None = None
    cwd: str = ""
    approval_mode: str = "buttons"  # "buttons" (Slack approve/deny) or "auto" (--allow-all)
    allowed_users: frozenset[str] = field(default_factory=frozenset)
    state_dir: Path = Path.home() / ".copilot-slack-gateway"
    max_session_age_hours: float = 18.0
    show_thoughts: bool = False
    permission_timeout_seconds: float = 300.0
    prompt_timeout_seconds: float = 3600.0

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

        return cls(
            slack_bot_token=bot,
            slack_app_token=app,
            copilot_bin=os.environ.get("COPILOT_BIN", "copilot").strip() or "copilot",
            model=(os.environ.get("COPILOT_MODEL") or os.environ.get("COPILOT_DEFAULT_MODEL") or "").strip() or None,
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
