"""Slack Bolt (Socket Mode) app that routes conversations to persistent Copilot ACP sessions."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
import uuid
from typing import Any

from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

from .acp import ACPSession, SessionDeadError
from .config import Config
from .sessions import Conversation, SessionRegistry

logger = logging.getLogger(__name__)

EDIT_THROTTLE_SECONDS = 1.5
LIVE_TEXT_LIMIT = 2800
FINAL_CHUNK_LIMIT = 3500
_SEEN_IDS_MAX = 5000
DENY_VALUE = "__deny__"
PERM_ACTION_ID = "perm_decision"


def chunk_text(text: str, limit: int = FINAL_CHUNK_LIMIT) -> list[str]:
    """Split text into <= limit chunks, preferring line boundaries."""
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        cut = remaining.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(remaining[:cut])
        remaining = remaining[cut:].lstrip("\n")
    if remaining or not chunks:
        chunks.append(remaining)
    return chunks


class StreamRenderer:
    """Renders one prompt's streamed output into Slack messages."""

    def __init__(self, client, channel: str, thread_ts: str | None, show_thoughts: bool) -> None:
        self.client = client
        self.channel = channel
        self.thread_ts = thread_ts  # None => top-level posts (DM conversations)
        self.show_thoughts = show_thoughts
        self.text_parts: list[str] = []
        self.thought_parts: list[str] = []
        self.tool_lines: list[str] = []
        self.message_ts: str | None = None
        self._last_edit = 0.0
        self._dirty = False

    async def _post(self, text: str, **kwargs) -> dict:
        if self.thread_ts is not None:
            kwargs.setdefault("thread_ts", self.thread_ts)
        return await self.client.chat_postMessage(channel=self.channel, text=text, **kwargs)

    async def start(self) -> None:
        resp = await self._post("⏳ Working…")
        self.message_ts = resp["ts"]

    def _render_live(self) -> str:
        sections: list[str] = []
        if self.tool_lines:
            shown = self.tool_lines[-8:]
            sections.append("\n".join(shown))
        text = "".join(self.text_parts)
        if text:
            if len(text) > LIVE_TEXT_LIMIT:
                text = "…" + text[-LIVE_TEXT_LIMIT:]
            sections.append(text)
        return "\n\n".join(sections)[:3900] or "⏳ Working…"

    async def _maybe_update(self, force: bool = False) -> None:
        if not self.message_ts:
            return
        now = time.monotonic()
        if not force and (not self._dirty or now - self._last_edit < EDIT_THROTTLE_SECONDS):
            return
        self._dirty = False
        self._last_edit = now
        with contextlib.suppress(Exception):
            await self.client.chat_update(
                channel=self.channel, ts=self.message_ts, text=self._render_live()
            )

    async def add_text(self, chunk: str) -> None:
        self.text_parts.append(chunk)
        self._dirty = True
        await self._maybe_update()

    async def add_thought(self, chunk: str) -> None:
        if self.show_thoughts:
            self.thought_parts.append(chunk)

    async def add_tool(self, title: str, status: str) -> None:
        line = f"🔧 {title}" + (f" — {status}" if status else "")
        if not self.tool_lines or self.tool_lines[-1].split(" — ")[0] != f"🔧 {title}":
            self.tool_lines.append(line)
        else:
            self.tool_lines[-1] = line
        self._dirty = True
        await self._maybe_update()

    async def finalize(self, error: str | None = None) -> None:
        full = "".join(self.text_parts).strip()
        thoughts = "".join(self.thought_parts).strip()
        if error:
            full = (full + "\n\n" if full else "") + f"⚠️ {error}"
        if not full:
            full = "_(no response)_"
        chunks = chunk_text(full)
        if self.message_ts:
            with contextlib.suppress(Exception):
                await self.client.chat_update(channel=self.channel, ts=self.message_ts, text=chunks[0])
            chunks = chunks[1:]
        for chunk in chunks:
            await self._post(chunk)
        if thoughts:
            for chunk in chunk_text(f"🧠 _reasoning_\n{thoughts}"):
                await self._post(chunk)


class PermissionManager:
    """Routes ACP session/request_permission prompts to Slack approve/deny buttons."""

    def __init__(self, client, timeout_seconds: float) -> None:
        self.client = client
        self.timeout = timeout_seconds
        self._pending: dict[str, asyncio.Future[str | None]] = {}

    async def request(self, conv: Conversation, params: dict[str, Any]) -> str | None:
        options = params.get("options") or []
        if not options:
            return None
        tool_call = params.get("toolCall") or {}
        title = str(tool_call.get("title") or "tool call")
        kind = str(tool_call.get("kind") or "")
        logger.info("permission requested in %s: %s (%s)", conv.key, title, kind)
        raw = tool_call.get("rawInput")
        raw_text = ""
        if raw:
            raw_text = "\n```" + json.dumps(raw, indent=2)[:1200] + "```"

        token = uuid.uuid4().hex[:12]
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str | None] = loop.create_future()
        self._pending[token] = fut

        # action_ids must be unique within the message; token+option travel in value.
        buttons = []
        for idx, opt in enumerate(options[:4]):
            option_id = str(opt.get("optionId") or "")
            name = str(opt.get("name") or opt.get("kind") or option_id)
            style = "primary" if "allow" in str(opt.get("kind") or "") else "danger"
            buttons.append({
                "type": "button",
                "text": {"type": "plain_text", "text": name[:75]},
                "action_id": f"{PERM_ACTION_ID}:{idx}",
                "value": f"{token}|{option_id or DENY_VALUE}",
                "style": style,
            })
        buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Deny"},
            "action_id": f"{PERM_ACTION_ID}:deny",
            "value": f"{token}|{DENY_VALUE}",
            "style": "danger",
        })

        kwargs = {"thread_ts": conv.thread_ts} if conv.thread_ts else {}
        await self.client.chat_postMessage(
            channel=conv.channel,
            text=f"🔐 Permission requested: {title}",
            **kwargs,
            blocks=[
                {"type": "section", "text": {"type": "mrkdwn",
                 "text": f"🔐 *Permission requested*\n`{kind}` {title}{raw_text}"}},
                {"type": "actions", "elements": buttons},
            ],
        )
        try:
            return await asyncio.wait_for(fut, timeout=self.timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                await self.client.chat_postMessage(
                    channel=conv.channel,
                    **({"thread_ts": conv.thread_ts} if conv.thread_ts else {}),
                    text=f"⏰ Permission request for `{title}` timed out — denied.",
                )
            return None
        finally:
            self._pending.pop(token, None)

    async def resolve(self, token: str, option_id: str | None) -> bool:
        fut = self._pending.get(token)
        if fut is None or fut.done():
            return False
        fut.set_result(option_id or None)
        return True


RESERVED_COMMANDS = {"/new", "/stop", "/tasks", "/help"}

HELP_TEXT = """*copilot-slack-gateway commands*
• just type — talk to Copilot (reuses this thread's persistent session)
• `/<skill> [args]` — invoke one of your Copilot skills (registered ones autocomplete)
• `/new` — discard this thread's Copilot session and start fresh
• `/stop` — cancel the currently running prompt
• `/tasks` — show gateway-visible active tasks without prompting Copilot
• `/help` — this message
"""


def skill_invocation_prompt(name: str, args: str) -> str:
    prompt = f'Invoke the "{name}" skill now using your skill tool.'
    if args:
        prompt += f"\n\nSkill arguments:\n{args}"
    return prompt


def format_task_status(status: dict[str, Any]) -> str:
    """Render gateway-visible ACP work without invoking the Copilot model."""
    session_id = status.get("session_id")
    if not session_id:
        return "📋 *Tasks*\nNo active Copilot session in this conversation."

    state = "running" if status.get("busy") else "idle"
    pid = status.get("pid")
    lines = [f"📋 *Tasks* — session `{session_id}` ({state})"]
    if pid:
        lines[0] += f", pid `{pid}`"
    queued = int(status.get("queued") or 0)
    if queued:
        lines.append(f"• Queued prompts: {queued}")
    tasks = status.get("tasks") or []
    if tasks:
        lines.append("• Active tool calls:")
        for task in tasks:
            title = " ".join(str(task.get("title") or "tool").split())
            task_status = " ".join(str(task.get("status") or "running").split())
            lines.append(f"  • {title} — {task_status}")
    elif status.get("busy"):
        lines.append("• Copilot is processing the current prompt.")
    else:
        lines.append("• No active subagents or shell commands.")
    return "\n".join(lines)


def build_app(config: Config) -> tuple[AsyncApp, SessionRegistry, PermissionManager]:
    app = AsyncApp(token=config.slack_bot_token)
    registry = SessionRegistry(config)
    permissions = PermissionManager(app.client, config.permission_timeout_seconds)
    seen_ids: list[str] = []

    def already_seen(event: dict[str, Any]) -> bool:
        eid = event.get("client_msg_id") or f"{event.get('channel')}:{event.get('ts')}"
        if eid in seen_ids:
            return True
        seen_ids.append(eid)
        if len(seen_ids) > _SEEN_IDS_MAX:
            del seen_ids[: _SEEN_IDS_MAX // 2]
        return False

    def authorized(user: str | None) -> bool:
        return bool(user) and user in config.allowed_users

    # ------------------------------------------------------- event plumbing

    async def post(conv: Conversation, text: str, **kwargs) -> None:
        if conv.thread_ts is not None:
            kwargs.setdefault("thread_ts", conv.thread_ts)
        await app.client.chat_postMessage(channel=conv.channel, text=text, **kwargs)

    def make_on_event(conv: Conversation):
        async def on_event(kind: str, data: dict[str, Any]) -> None:
            renderer: StreamRenderer | None = getattr(conv, "renderer", None)
            if kind == "text" and renderer:
                await renderer.add_text(data["text"])
            elif kind == "thought" and renderer:
                await renderer.add_thought(data["text"])
            elif kind == "tool" and renderer:
                await renderer.add_tool(data["title"], data.get("status") or "")
            elif kind == "error":
                with contextlib.suppress(Exception):
                    await post(conv, f"⚠️ {data['message']}")
        return on_event

    def make_permission_handler(conv: Conversation):
        if config.approval_mode == "auto":
            async def auto_handler(params: dict[str, Any]) -> str | None:
                for opt in params.get("options") or []:
                    if "allow" in str(opt.get("kind") or ""):
                        return str(opt.get("optionId") or "") or None
                return None
            return auto_handler

        async def handler(params: dict[str, Any]) -> str | None:
            return await permissions.request(conv, params)
        return handler

    @app.action(re.compile(r"^perm_decision:"))
    async def handle_permission_action(ack, body, respond):
        await ack()
        token, _, raw_value = (body["actions"][0].get("value") or "|").partition("|")
        option_id = None if raw_value in ("", DENY_VALUE) else raw_value
        resolved = await permissions.resolve(token, option_id)
        label = "approved" if option_id else "denied"
        original = body.get("message", {}).get("text", "Permission request")
        await respond(text=f"{original}\n— _{label} by <@{body['user']['id']}>_" if resolved else f"{original}\n— _(already resolved)_")

    # ----------------------------------------------------------- prompt flow

    async def run_prompt(conv: Conversation, text: str) -> None:
        renderer = StreamRenderer(app.client, conv.channel, conv.thread_ts, config.show_thoughts)
        conv.renderer = renderer
        error: str | None = None
        try:
            await renderer.start()
            session: ACPSession = conv.session
            if not session.alive:
                await session.start()
                await post(conv, f"🟢 Copilot session started (`{session.session_id}`, pid {session.pid}, model `{session.model or 'cli default'}`)")
            await session.prompt(text)
        except SessionDeadError as exc:
            error = f"Copilot session died: {exc}. Send another message to start a fresh session."
        except asyncio.TimeoutError:
            error = "prompt timed out. Use /stop or /new if it wedged."
        except Exception as exc:
            logger.exception("prompt failed for %s", conv.key)
            error = f"{type(exc).__name__}: {exc}"
        finally:
            conv.renderer = None
            await renderer.finalize(error=error)

    async def worker(conv: Conversation) -> None:
        while True:
            text = await conv.queue.get()
            if text is None:  # shutdown sentinel
                return
            try:
                await run_prompt(conv, text)
            except Exception:
                logger.exception("worker iteration failed for %s", conv.key)
            finally:
                conv.queue.task_done()

    async def dispatch_prompt(conv: Conversation, text: str, fresh: bool) -> None:
        if conv.worker is None or conv.worker.done():
            conv.worker = asyncio.create_task(worker(conv), name=f"worker-{conv.key}")
        depth = conv.queue.qsize()
        if fresh:
            await post(conv, "♻️ Previous Copilot session expired or stopped — starting a new one.")
        if depth > 0:
            await post(conv, f"⏳ Still working — your message is queued (position {depth + 1}).")
        conv.queue.put_nowait(text)

    async def handle_text(text: str, channel: str, thread_ts: str | None, user: str, *, is_dm: bool) -> None:
        text = text.strip()
        if not text:
            return
        key = SessionRegistry.key_for(channel, thread_ts, is_dm=is_dm)
        reply_thread = None if is_dm else thread_ts
        reply_conv = Conversation(key=key, channel=channel, thread_ts=reply_thread, cwd=config.cwd)
        if text.startswith("/"):
            command, _, arg = text.partition(" ")
            command, arg = command.lower(), arg.strip()
            if command == "/new":
                dropped = await registry.reset(key)
                await post(reply_conv,
                    "🆕 Session cleared — next message starts fresh." if dropped
                    else "No active session here — your next message will start one.")
                return
            if command == "/stop":
                cancelled = await registry.cancel(key)
                await post(reply_conv,
                    "🛑 Cancellation sent." if cancelled else "Nothing is running here.")
                return
            if command == "/help":
                await post(reply_conv, HELP_TEXT)
                return
            if command == "/tasks":
                # Copilot's interactive /tasks is unavailable over ACP; keep this local.
                await post(reply_conv, format_task_status(registry.task_status(key)))
                return
            # Not a gateway command — treat as a Copilot skill invocation.
            text = skill_invocation_prompt(command.lstrip("/"), arg)
        conv, fresh = await registry.get_or_create(
            key,
            channel=channel,
            thread_ts=reply_thread,
            on_event_factory=make_on_event,
            permission_handler_factory=make_permission_handler,
        )
        await dispatch_prompt(conv, text, fresh)

    # --------------------------------------------------------- event handlers

    @app.event("message")
    async def on_message(event, say):
        if event.get("subtype") or event.get("bot_id"):
            return
        if event.get("channel_type") != "im":
            return
        if not authorized(event.get("user")) or already_seen(event):
            return
        await handle_text(
            event.get("text") or "",
            event["channel"],
            event.get("thread_ts") or event["ts"],
            event["user"],
            is_dm=True,
        )

    @app.command(re.compile(r"^/[a-z0-9][a-z0-9_-]*$"))
    async def on_slash_command(ack, command, respond):
        await ack()
        if not authorized(command.get("user_id")):
            return
        is_dm = command.get("channel_name") == "directmessage"
        if not is_dm:
            await respond(response_type="ephemeral",
                          text="Run skills in a DM with me, or @mention me in a thread.")
            return
        text = f"{command['command']} {command.get('text') or ''}".strip()
        await handle_text(text, command["channel_id"], None, command["user_id"], is_dm=True)

    @app.event("app_mention")
    async def on_mention(event, say):
        if not authorized(event.get("user")) or already_seen(event):
            return
        text = re.sub(r"<@[A-Z0-9]+>", "", event.get("text") or "", count=1).strip()
        await handle_text(
            text,
            event["channel"],
            event.get("thread_ts") or event["ts"],
            event["user"],
            is_dm=False,
        )

    return app, registry, permissions


async def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config.from_env()
    app, registry, _permissions = build_app(config)
    handler = AsyncSocketModeHandler(app, config.slack_app_token)
    logger.info("starting copilot-slack-gateway (cwd=%s, model=%s, approvals=%s)",
                config.cwd, config.model or "cli default", config.approval_mode)

    async def shutdown() -> None:
        logger.info("shutting down: closing copilot sessions")
        await registry.close_all()

    try:
        await handler.start_async()
    finally:
        await shutdown()
