"""Conversation -> persistent ACP session registry.

Each Slack conversation (a DM thread, or a channel thread started by an
@mention) gets its own long-lived Copilot session. Sessions are recycled when
they exceed MAX_SESSION_AGE_HOURS (the "daily rollover") or when their process
has died.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .acp import ACPSession, EventCallback, PermissionHandler
from .config import Config

logger = logging.getLogger(__name__)


@dataclass
class Conversation:
    key: str
    channel: str
    thread_ts: str | None  # None => post top-level (DM conversations)
    cwd: str
    session: ACPSession | None = None
    created_at: float = 0.0
    queue: asyncio.Queue[str] = field(default_factory=asyncio.Queue)
    worker: asyncio.Task[None] | None = None

    def stale(self, max_age_hours: float) -> bool:
        if self.session is None or not self.session.alive:
            return True
        return (time.time() - self.created_at) > max_age_hours * 3600


class SessionRegistry:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._conversations: dict[str, Conversation] = {}
        self._lock = asyncio.Lock()
        self.config.state_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def key_for(channel: str, thread_ts: str | None, *, is_dm: bool = False) -> str:
        # A DM is one long-lived conversation (top-level replies); a channel
        # conversation is scoped to the thread the bot was mentioned in.
        return channel if is_dm else f"{channel}:{thread_ts}"

    @property
    def _state_path(self) -> Path:
        return self.config.state_dir / "state.json"

    def load_metadata(self) -> dict[str, Any]:
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_metadata(self) -> None:
        data = {
            key: {
                "channel": conv.channel,
                "thread_ts": conv.thread_ts,
                "cwd": conv.cwd,
                "created_at": conv.created_at,
                "session_id": conv.session.session_id if conv.session else None,
            }
            for key, conv in self._conversations.items()
        }
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self._state_path)

    async def get_or_create(
        self,
        key: str,
        *,
        channel: str,
        thread_ts: str | None,
        on_event_factory,
        permission_handler_factory,
    ) -> tuple[Conversation, bool]:
        """Return the conversation; True when a fresh Copilot session is needed.

        on_event_factory(conv) -> EventCallback
        permission_handler_factory(conv) -> PermissionHandler
        """
        async with self._lock:
            conv = self._conversations.get(key)
            if conv is None:
                conv = Conversation(key=key, channel=channel, thread_ts=thread_ts, cwd=self.config.cwd)
                self._conversations[key] = conv
            # "recycled" = a previous session existed and is being replaced;
            # a brand-new conversation is not a recycle.
            recycled = conv.session is not None and conv.stale(self.config.max_session_age_hours)
            if recycled and conv.session is not None:
                logger.info("recycling session for %s (alive=%s, age=%.1fh)",
                            key,
                            conv.session.alive,
                            (time.time() - conv.created_at) / 3600)
                await conv.session.close()
                conv.session = None
            if conv.session is None:
                conv.session = ACPSession(
                    command=self.config.copilot_bin,
                    args=self.config.copilot_args,
                    cwd=conv.cwd,
                    model=self.config.model,
                    on_event=on_event_factory(conv),
                    permission_handler=permission_handler_factory(conv),
                    prompt_timeout=self.config.prompt_timeout_seconds,
                )
                conv.created_at = time.time()
                self._save_metadata()
            return conv, recycled

    async def reset(self, key: str) -> bool:
        """Close and drop the Copilot session for a conversation ("/new")."""
        async with self._lock:
            conv = self._conversations.get(key)
            if conv is None or conv.session is None:
                return False
            await conv.session.close()
            conv.session = None
            conv.created_at = 0.0
            self._save_metadata()
            return True

    async def cancel(self, key: str) -> bool:
        conv = self._conversations.get(key)
        if conv and conv.session and conv.session.busy():
            await conv.session.cancel()
            return True
        return False

    def task_status(self, key: str) -> dict[str, Any]:
        """Return gateway-visible work for a conversation without prompting Copilot."""
        conv = self._conversations.get(key)
        if conv is None or conv.session is None or not conv.session.alive:
            return {"session_id": None, "pid": None, "busy": False, "queued": 0, "tasks": []}
        return {
            "session_id": conv.session.session_id,
            "pid": conv.session.pid,
            "busy": conv.session.busy(),
            "queued": conv.queue.qsize(),
            "tasks": conv.session.active_tasks(),
        }

    async def close_all(self) -> None:
        async with self._lock:
            for conv in self._conversations.values():
                if conv.session:
                    await conv.session.close()
            self._conversations.clear()
