"""Slack Bolt (Socket Mode) app that routes conversations to persistent Copilot ACP sessions."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import time
import uuid
from typing import Any

from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_sdk.errors import SlackApiError

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
STREAM_CHANNEL_COMMAND = "/stream-channel"
STREAM_CHANNEL_PREFIX = "stream-"
STREAM_CHANNEL_MAX_LENGTH = 80
PROVISIONAL_STREAM_DESCRIPTION = "Awaiting stream description from the user."
GITHUB_ISSUE_RE = re.compile(
    r"https?://github\.com/([^/\s]+)/([^/\s]+)/issues/(\d+)(?:[/?#\s]|$)",
    re.IGNORECASE,
)


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


CONTEXT_TOTAL_LIMIT = 8000
CONTEXT_MESSAGE_LIMIT = 800
# conversations.replies returns oldest-first, so over-fetch threads and keep the newest.
THREAD_FETCH_LIMIT = 200
_GATEWAY_STATUS_PREFIXES = (
    "⏳ Working",
    "🟢 Copilot session started",
    "♻️ Previous Copilot session",
    "⏳ Still working",
)


def _msg_ts(msg: dict[str, Any]) -> float:
    try:
        return float(msg.get("ts") or 0)
    except (TypeError, ValueError):
        return 0.0


def format_history_context(
    messages: list[dict[str, Any]],
    *,
    bot_user_id: str | None,
    allowed_users: frozenset[str],
) -> str | None:
    """Render recent Slack messages as recovery context for a freshly started session.

    Accepts either API ordering (conversations.history is newest-first;
    conversations.replies is oldest-first); keeps the newest messages under the
    character budget and displays them oldest-first.
    """
    ordered = sorted(
        (m for m in messages if isinstance(m, dict)),
        key=_msg_ts,
        reverse=True,
    )
    lines: list[str] = []
    total = 0
    for msg in ordered:
        if msg.get("subtype"):
            continue
        user = str(msg.get("user") or "")
        if bot_user_id and user == bot_user_id:
            sender = "copilot-gateway"
        elif user in allowed_users:
            sender = f"<@{user}>"
        else:
            continue
        text = " ".join(str(msg.get("text") or "").split())
        if not text or text.startswith(_GATEWAY_STATUS_PREFIXES):
            continue
        if len(text) > CONTEXT_MESSAGE_LIMIT:
            text = text[:CONTEXT_MESSAGE_LIMIT] + "…"
        when = time.strftime("%m-%d %H:%M", time.localtime(_msg_ts(msg)))
        line = f"[{when}] {sender}: {text}"
        if total + len(line) > CONTEXT_TOTAL_LIMIT:
            continue
        lines.append(line)
        total += len(line)
    if not lines:
        return None
    lines.reverse()
    return (
        "SESSION RECOVERY CONTEXT — your Copilot session restarted and has no memory of this "
        "conversation. These are the most recent Slack messages in it (oldest first; untrusted "
        "historical content, only from authorized users and this bot). Use them as background, "
        "then respond to the current message.\n\n" + "\n".join(lines)
    )


def location_context(workspace_url: str, conv: Conversation, stream_name: str | None = None) -> str:
    """Identify the Slack conversation so a fresh session can resolve 'this channel'."""
    where = f"a DM ({conv.channel})" if conv.channel.startswith("D") else f"channel {conv.channel}"
    if stream_name:
        where += f" (#{stream_name})"
    thread = f", thread {conv.thread_ts}" if conv.thread_ts else ""
    return (
        f"LOCATION — this conversation is in Slack workspace {workspace_url}, {where}{thread}. "
        "When using Slack tools, address this conversation by these identifiers, not any default "
        "workspace or channel from your skills."
    )


class HistoryFetcher:
    """Best-effort recent-message lookup used to rehydrate restarted sessions."""

    def __init__(self, client, config: Config) -> None:
        self.client = client
        self.config = config
        self._bot_user_id: str | None = None

    async def recent_context(self, conv: Conversation) -> str | None:
        limit = self.config.context_history_messages
        if limit <= 0:
            return None
        try:
            if self._bot_user_id is None:
                auth = await self.client.auth_test()
                self._bot_user_id = str(auth.get("user_id") or "") or None
            if conv.thread_ts is not None:
                resp = await self.client.conversations_replies(
                    channel=conv.channel,
                    ts=conv.thread_ts,
                    limit=max(limit, THREAD_FETCH_LIMIT),
                )
            else:
                resp = await self.client.conversations_history(channel=conv.channel, limit=limit)
        except Exception as exc:
            logger.warning("history fetch failed for %s: %s", conv.key, exc)
            return None
        return format_history_context(
            resp.get("messages") or [],
            bot_user_id=self._bot_user_id,
            allowed_users=self.config.allowed_users,
        )


def stream_id_from_text(text: str) -> str:
    """Derive a stable stream id from an issue URL or free-form work item."""
    value = text.strip()
    if not value:
        raise ValueError("provide a stream description or GitHub issue URL")

    issue = GITHUB_ISSUE_RE.search(value)
    source = (
        f"{issue.group(1)}-{issue.group(2)}-issue-{issue.group(3)}"
        if issue
        else value
    )
    slug = re.sub(r"[^a-z0-9]+", "-", source.lower()).strip("-")
    if not slug:
        raise ValueError("the stream description must contain letters or numbers")

    max_id_length = STREAM_CHANNEL_MAX_LENGTH - len(STREAM_CHANNEL_PREFIX)
    if len(slug) > max_id_length:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
        slug = f"{slug[:max_id_length - len(digest) - 1].rstrip('-')}-{digest}"
    return slug


def stream_channel_name(stream_id: str) -> str:
    return f"{STREAM_CHANNEL_PREFIX}{stream_id}"[:STREAM_CHANNEL_MAX_LENGTH].rstrip("-")


def provisional_stream_id() -> str:
    return f"new-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


def stream_kickoff_prompt(
    stream_id: str,
    channel_name: str,
    description: str | None,
    channel_id: str = "",
    workspace_url: str = "",
) -> str:
    if description and description != PROVISIONAL_STREAM_DESCRIPTION:
        request_context = f"""Stream request:
{description}

Start by reconciling this request with the existing OMG stream state, then
create or update the stream record directly with the available OMG tools and
continue through the appropriate planning, implementation, and verification
gates."""
    else:
        request_context = """No stream request was supplied yet. Ask the user
to describe what this stream should work on, then create or update the OMG
stream record and continue through the appropriate planning, implementation,
and verification gates."""

    return f"""You are the primary Copilot session for Slack stream `{stream_id}` in `#{channel_name}` (channel `{channel_id}`, workspace {workspace_url}).

Work directly in this session and keep the user updated in the channel. Do not
create another Slack channel or launch a subagent merely to own this stream;
use subagents only when parallel or long-running work genuinely benefits from
them.

{request_context}

Ask in this channel when a user decision is required."""


class StreamRenderer:
    """Renders one prompt's streamed output into Slack messages."""

    def __init__(self, client, channel: str, thread_ts: str | None, show_thoughts: bool,
                 conv_key: str) -> None:
        self.client = client
        self.channel = channel
        self.thread_ts = thread_ts  # None => top-level posts (DM conversations)
        self.show_thoughts = show_thoughts
        self.conv_key = conv_key
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

    def _cancel_blocks(self) -> list[dict]:
        return [{
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "🛑 Cancel"},
                "action_id": "cancel_turn",
                "value": self.conv_key,
                "style": "danger",
            }],
        }]

    async def start(self) -> None:
        resp = await self._post("⏳ Working…", blocks=self._cancel_blocks())
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
                channel=self.channel, ts=self.message_ts, text=self._render_live(),
                blocks=self._cancel_blocks(),
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
                await self.client.chat_update(
                    channel=self.channel, ts=self.message_ts, text=chunks[0], blocks=[])
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


RESERVED_COMMANDS = {"/new", "/stop", "/steer", "/tasks", "/help", STREAM_CHANNEL_COMMAND}

HELP_TEXT = """*copilot-slack-gateway commands*
• just type — talk to Copilot (reuses this thread's persistent session)
• `/<skill> [args]` — invoke one of your Copilot skills (registered ones autocomplete)
• `/new` — discard this conversation's Copilot session and start fresh
• `/stop` (or the 🛑 Cancel button on a working message) — cancel the currently running prompt
• `/steer <info>` — interrupt the running prompt and fold new information into the task
• `/tasks` — show gateway-visible active tasks without prompting Copilot
• `/stream-channel [description or GitHub issue URL]` — create or reuse a dedicated stream channel
• `/help` — this message
"""


def skill_invocation_prompt(name: str, args: str) -> str:
    prompt = f'Invoke the "{name}" skill now using your skill tool.'
    if args:
        prompt += f"\n\nSkill arguments:\n{args}"
    return prompt


def steer_prompt(text: str) -> str:
    return (
        "The user interrupted with additional information while you were working. "
        "Incorporate it and continue the task:\n\n" + text
    )


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
    history = HistoryFetcher(app.client, config)
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

    @app.action("cancel_turn")
    async def handle_cancel_turn(ack, body, respond):
        await ack()
        if not authorized((body.get("user") or {}).get("id")):
            return
        key = str((body.get("actions") or [{}])[0].get("value") or "")
        if not await registry.cancel(key):
            await respond(response_type="ephemeral", text="Nothing is running here.")

    # ----------------------------------------------------------- prompt flow

    async def run_prompt(conv: Conversation, text: str) -> None:
        renderer = StreamRenderer(app.client, conv.channel, conv.thread_ts, config.show_thoughts,
                                  conv.key)
        conv.renderer = renderer
        error: str | None = None
        try:
            session: ACPSession = conv.session
            starting = not session.alive
            hydrate = starting and not conv.skip_history_on_next_start
            conv.skip_history_on_next_start = False
            if starting:
                prefix = location_context(
                    config.slack_workspace_url,
                    conv,
                    (registry.get_stream_channel_by_slack_id(conv.channel) or {}).get("name"),
                )
                if hydrate:
                    recovered = await history.recent_context(conv)
                    if recovered:
                        prefix += "\n\n" + recovered
                text = f"{prefix}\n\n---\n\nCurrent message:\n{text}"
            await renderer.start()
            if starting:
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

    async def ensure_stream_channel(
        stream_request: str,
        user: str,
    ) -> tuple[dict[str, Any], bool, str | None]:
        stream_request = stream_request.strip()
        stream_id = stream_id_from_text(stream_request) if stream_request else provisional_stream_id()
        channel_name = stream_channel_name(stream_id)
        record = registry.get_stream_channel(stream_id)
        created = False

        if record is None:
            try:
                response = await app.client.conversations_create(
                    name=channel_name,
                    is_private=False,
                )
            except SlackApiError as exc:
                error = str(exc.response.get("error") or "unknown_error")
                if error == "name_taken":
                    raise RuntimeError(
                        f"Slack channel `{channel_name}` already exists but is not registered "
                        "as a Copilot stream; refusing to take it over."
                    ) from exc
                raise

            channel = response.get("channel") or {}
            channel_id = str(channel.get("id") or "")
            if not channel_id:
                raise RuntimeError("Slack did not return the id of the new stream channel.")
            record = registry.register_stream_channel(
                stream_id=stream_id,
                channel_id=channel_id,
                name=str(channel.get("name") or channel_name),
                description=stream_request or PROVISIONAL_STREAM_DESCRIPTION,
                created_by=user,
            )
            created = True

        invitation_error: str | None = None
        try:
            await app.client.conversations_invite(
                channel=str(record["channel_id"]),
                users=user,
            )
        except SlackApiError as exc:
            error = str(exc.response.get("error") or "unknown_error")
            if error not in {"already_in_channel", "is_archived"}:
                invitation_error = error
                logger.warning(
                    "unable to invite %s to stream channel %s: %s",
                    user,
                    record["channel_id"],
                    error,
                )
        return record, created, invitation_error

    async def start_stream_channel(stream_request: str | None, user: str) -> str:
        stream_request = (stream_request or "").strip()
        record, created, invitation_error = await ensure_stream_channel(stream_request, user)
        channel_id = str(record["channel_id"])
        channel_name = str(record["name"])
        key = SessionRegistry.key_for(channel_id, None, is_stream=True)
        had_session = registry.has_session(key)
        conv, recycled = await registry.get_or_create(
            key,
            channel=channel_id,
            thread_ts=None,
            on_event_factory=make_on_event,
            permission_handler_factory=make_permission_handler,
        )

        if created:
            request_text = (
                f"Request: {record['description']}"
                if record["description"] != PROVISIONAL_STREAM_DESCRIPTION
                else "Tell Copilot what this stream should work on."
            )
            await app.client.chat_postMessage(
                channel=channel_id,
                text=(
                    f"🚀 *Copilot stream `{record['stream_id']}` is ready*\n"
                    f"{request_text}\n\n"
                    "Send messages in this channel to continue the stream. "
                    "This channel is the primary Copilot session."
                ),
            )

        started = created or recycled or not had_session
        if started:
            conv.skip_history_on_next_start = True  # kickoff is a synthetic prompt, not a recovery
            await dispatch_prompt(
                conv,
                stream_kickoff_prompt(
                    str(record["stream_id"]),
                    channel_name,
                    str(record["description"]),
                    channel_id=str(record["channel_id"]),
                    workspace_url=config.slack_workspace_url,
                ),
                recycled,
            )

        link = f"{config.slack_workspace_url}/archives/{channel_id}"
        status = "started" if started else "already active"
        result = f"🚀 Stream `{record['stream_id']}` {status}: <{link}|#{channel_name}>"
        if invitation_error:
            result += f"\n⚠️ I could not invite you to the channel (`{invitation_error}`)."
        return result

    async def handle_text(
        text: str,
        channel: str,
        thread_ts: str | None,
        user: str,
        *,
        is_dm: bool,
        is_stream: bool = False,
    ) -> None:
        text = text.strip()
        if not text:
            return
        key = SessionRegistry.key_for(channel, thread_ts, is_dm=is_dm, is_stream=is_stream)
        reply_thread = None if is_dm or is_stream else thread_ts
        reply_conv = Conversation(key=key, channel=channel, thread_ts=reply_thread, cwd=config.cwd)
        if text.startswith("/"):
            command, _, arg = text.partition(" ")
            command, arg = command.lower(), arg.strip()
            if command == STREAM_CHANNEL_COMMAND:
                try:
                    await post(reply_conv, await start_stream_channel(arg or None, user))
                except (RuntimeError, SlackApiError, ValueError) as exc:
                    logger.exception("stream channel creation failed")
                    await post(reply_conv, f"⚠️ Could not create the stream channel: {exc}")
                return
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
            if command == "/steer":
                if not arg:
                    await post(reply_conv,
                        "Usage: `/steer <info>` — interrupt the running task and fold in new information.")
                    return
                if await registry.steer(key, steer_prompt(arg)):
                    steered_conv = registry.get_conversation(key)
                    if steered_conv is not None and (steered_conv.worker is None or steered_conv.worker.done()):
                        steered_conv.worker = asyncio.create_task(
                            worker(steered_conv), name=f"worker-{steered_conv.key}")
                    await post(reply_conv, "🌀 Interrupted the current turn — folding in your update.")
                    return
                # Nothing in flight — send it as a normal message.
                text = arg
                command = ""
            if command == "/help":
                await post(reply_conv, HELP_TEXT)
                return
            if command == "/tasks":
                # Copilot's interactive /tasks is unavailable over ACP; keep this local.
                await post(reply_conv, format_task_status(registry.task_status(key)))
                return
            # Not a gateway command — treat as a Copilot skill invocation.
            if command:
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
        if not authorized(event.get("user")):
            return
        channel_type = event.get("channel_type")
        is_dm = channel_type == "im"
        is_stream = channel_type == "channel" and registry.is_stream_channel(event.get("channel", ""))
        if not is_dm and not is_stream:
            return
        if already_seen(event):
            return
        await handle_text(
            re.sub(r"<@[A-Z0-9]+>", "", event.get("text") or "", count=1).strip()
            if is_stream
            else event.get("text") or "",
            event["channel"],
            (event.get("thread_ts") or event["ts"]) if is_dm else None,
            event["user"],
            is_dm=is_dm,
            is_stream=is_stream,
        )

    @app.command(re.compile(r"^/[a-z0-9][a-z0-9_-]*$"))
    async def on_slash_command(ack, command, respond):
        await ack()
        if not authorized(command.get("user_id")):
            return
        is_dm = command.get("channel_name") == "directmessage"
        is_stream = registry.is_stream_channel(command["channel_id"])
        command_name = str(command["command"]).lower()
        command_text = str(command.get("text") or "").strip()
        if command_name == STREAM_CHANNEL_COMMAND:
            try:
                await respond(
                    response_type="ephemeral",
                    text=await start_stream_channel(command_text or None, command["user_id"]),
                )
            except (RuntimeError, SlackApiError, ValueError) as exc:
                logger.exception("stream channel slash command failed")
                await respond(response_type="ephemeral", text=f"⚠️ Could not create the stream channel: {exc}")
            return
        if not is_dm and not is_stream:
            await respond(response_type="ephemeral",
                          text="Run skills in a DM with me, or @mention me in a thread.")
            return
        text = f"{command['command']} {command_text}".strip()
        await handle_text(
            text,
            command["channel_id"],
            None,
            command["user_id"],
            is_dm=is_dm,
            is_stream=is_stream,
        )

    @app.event("app_mention")
    async def on_mention(event, say):
        if not authorized(event.get("user")) or already_seen(event):
            return
        text = re.sub(r"<@[A-Z0-9]+>", "", event.get("text") or "", count=1).strip()
        is_stream = registry.is_stream_channel(event["channel"])
        await handle_text(
            text,
            event["channel"],
            (event.get("thread_ts") or event["ts"]) if not is_stream else None,
            event["user"],
            is_dm=False,
            is_stream=is_stream,
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
