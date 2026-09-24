"""Persistent Agent Client Protocol (ACP) client for `copilot --acp --stdio`.

This client keeps the Copilot process and its ACP session alive across many
prompts, so a Slack conversation reuses the same Copilot session all day.

Wire format: newline-delimited JSON-RPC 2.0 over stdio.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

_INITIALIZE_PARAMS = {
    "protocolVersion": 1,
    "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}},
    "clientInfo": {"name": "copilot-slack-gateway", "title": "Copilot Slack Gateway", "version": "0.1.0"},
}

# on_event(kind, data): kinds are "text", "thought", "tool", "plan", "done", "error"
EventCallback = Callable[[str, dict[str, Any]], Awaitable[None]]
# permission_handler(params) -> optionId to select, or None to cancel
PermissionHandler = Callable[[dict[str, Any]], Awaitable[str | None]]


class SessionDeadError(RuntimeError):
    """The Copilot ACP process exited (or never started)."""


def _enabled_ids(entries: Any, key: str) -> set[str]:
    return {
        str(e.get(key) or "").strip()
        for e in (entries or [])
        if isinstance(e, dict)
        and str((e.get("_meta") or {}).get("copilotEnablement") or "").strip().lower() != "disabled"
    }


def _model_selection_request(session: dict[str, Any], model: str) -> tuple[str, dict[str, Any]] | None:
    """Build the ACP request selecting `model`, or None if unavailable/unnecessary."""
    session_id = str(session.get("sessionId") or "").strip()
    if not session_id or not model:
        return None
    options = [
        o for o in (session.get("configOptions") or [])
        if isinstance(o, dict) and "model" in (o.get("category"), o.get("id"))
    ]
    if options:
        if model not in _enabled_ids(options[0].get("options"), "value"):
            return None
        return "session/set_config_option", {
            "sessionId": session_id,
            "configId": str(options[0].get("id") or "model"),
            "value": model,
        }
    available = _enabled_ids((session.get("models") or {}).get("availableModels"), "modelId")
    if available and model not in available:
        return None
    return "session/set_model", {"sessionId": session_id, "modelId": model}


def _ensure_within_cwd(path_text: str, cwd: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        raise PermissionError("ACP file-system paths must be absolute.")
    resolved, root = path.resolve(), Path(cwd).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PermissionError(f"Path '{resolved}' is outside the session cwd '{root}'.") from exc
    return resolved


class ACPSession:
    """One persistent `copilot --acp --stdio` process hosting one ACP session."""

    def __init__(
        self,
        *,
        command: str,
        args: tuple[str, ...],
        cwd: str,
        model: str | None,
        mcp_servers: list[dict[str, Any]] | None = None,
        on_event: EventCallback,
        permission_handler: PermissionHandler,
        prompt_timeout: float = 3600.0,
    ) -> None:
        self.command = command
        self.args = args
        self.cwd = cwd
        self.model = model
        self.mcp_servers = list(mcp_servers or [])
        self.on_event = on_event
        self.permission_handler = permission_handler
        self.prompt_timeout = prompt_timeout

        self.session_id: str | None = None
        self.created_at: float = 0.0
        self.pid: int | None = None

        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._active_tools: dict[str, dict[str, str]] = {}
        self._next_id = itertools.count(1)
        self._write_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()
        self._stopping = False

    # ------------------------------------------------------------------ utils

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.returncode is None and self.session_id is not None

    def busy(self) -> bool:
        return self._prompt_lock.locked()

    def active_tasks(self) -> list[dict[str, str]]:
        """Return the currently active ACP tool calls without exposing inputs."""
        return [dict(task) for task in self._active_tools.values()]

    async def _write(self, payload: dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise SessionDeadError("ACP process is not running.")
        async with self._write_lock:
            self._proc.stdin.write(json.dumps(payload).encode() + b"\n")
            await self._proc.stdin.drain()

    async def _request(self, method: str, params: dict[str, Any], *, timeout: float = 60.0) -> Any:
        request_id = next(self._next_id)
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = fut
        try:
            await self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(request_id, None)

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"jsonrpc": "2.0", "method": method, "params": params})

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self.alive:
            return
        self._stopping = False
        try:
            self._proc = await asyncio.create_subprocess_exec(
                self.command,
                *self.args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.cwd,
                limit=16 * 1024 * 1024,
            )
        except FileNotFoundError as exc:
            raise SessionDeadError(f"Copilot binary not found: {self.command!r}") from exc
        self.pid = self._proc.pid
        self._reader_task = asyncio.create_task(self._read_loop(), name=f"acp-reader-{self.pid}")
        self._stderr_task = asyncio.create_task(self._stderr_loop(), name=f"acp-stderr-{self.pid}")

        await self._request("initialize", _INITIALIZE_PARAMS)
        session = await self._request("session/new", {"cwd": self.cwd, "mcpServers": self.mcp_servers})
        self.session_id = str(session.get("sessionId") or "").strip()
        if not self.session_id:
            raise SessionDeadError("session/new did not return a sessionId.")
        self.created_at = time.time()

        if self.model:
            selection = _model_selection_request(session, self.model)
            if selection is not None:
                try:
                    await self._request(*selection)
                    logger.info("session %s: model set to %s", self.session_id, self.model)
                except Exception as exc:
                    logger.warning("model selection for %r failed; using session default: %s", self.model, exc)
            else:
                logger.warning("model %r not offered by this CLI; using session default", self.model)

    async def close(self) -> None:
        self._stopping = True
        proc, self._proc = self._proc, None
        self.session_id = None
        self._active_tools.clear()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(SessionDeadError("session closed"))
        self._pending.clear()
        for task in (self._reader_task, self._stderr_task):
            if task:
                task.cancel()
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3)
            except (asyncio.TimeoutError, ProcessLookupError):
                with contextlib.suppress(Exception):
                    proc.kill()

    # --------------------------------------------------------------- prompting

    async def prompt(self, text: str) -> dict[str, Any]:
        """Send one user prompt; streams updates via on_event. Serialized per session."""
        async with self._prompt_lock:
            if not self.alive:
                raise SessionDeadError("ACP session is not alive.")
            result = await self._request(
                "session/prompt",
                {"sessionId": self.session_id, "prompt": [{"type": "text", "text": text}]},
                timeout=self.prompt_timeout,
            )
            return result or {}

    async def cancel(self) -> None:
        if self.alive:
            try:
                await self._notify("session/cancel", {"sessionId": self.session_id})
            except Exception:
                logger.exception("failed to send session/cancel")

    # ------------------------------------------------------------- read loops

    async def _read_loop(self) -> None:
        proc = self._proc
        assert proc is not None and proc.stdout is not None
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    logger.debug("non-JSON stdout from copilot: %s", line[:200])
                    continue
                asyncio.create_task(self._dispatch(msg))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ACP reader loop failed")
        finally:
            self._on_process_exit()

    async def _stderr_loop(self) -> None:
        proc = self._proc
        assert proc is not None and proc.stderr is not None
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                logger.info("copilot stderr: %s", line.decode(errors="replace").rstrip()[:500])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("stderr loop ended", exc_info=True)

    def _on_process_exit(self) -> None:
        if self._stopping:
            return
        logger.warning("copilot ACP process exited (pid=%s, session=%s)", self.pid, self.session_id)
        self.session_id = None
        self._active_tools.clear()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(SessionDeadError("copilot ACP process exited"))
        self._pending.clear()
        try:
            asyncio.get_running_loop().create_task(
                self.on_event("error", {"message": "Copilot process exited; the next message will start a fresh session."})
            )
        except RuntimeError:
            pass

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        method = msg.get("method")
        if isinstance(method, str) and "id" in msg:
            await self._handle_server_request(msg)
        elif isinstance(method, str):
            await self._handle_notification(method, msg.get("params") or {})
        elif "id" in msg:
            fut = self._pending.get(msg["id"])
            if fut is None or fut.done():
                return
            if "error" in msg:
                err = msg.get("error") or {}
                fut.set_exception(RuntimeError(f"ACP error {err.get('code')}: {err.get('message') or err}"))
            else:
                fut.set_result(msg.get("result"))

    async def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if method != "session/update":
            logger.debug("ignoring notification %s", method)
            return
        update = params.get("update") or {}
        kind = str(update.get("sessionUpdate") or "")
        content = update.get("content") or {}
        text = str(content.get("text") or "") if isinstance(content, dict) else ""
        if kind == "agent_message_chunk" and text:
            await self.on_event("text", {"text": text})
        elif kind == "agent_thought_chunk" and text:
            await self.on_event("thought", {"text": text})
        elif kind in ("tool_call", "tool_call_update"):
            tool_call_id = str(update.get("toolCallId") or "").strip()
            if tool_call_id:
                task = self._active_tools.setdefault(tool_call_id, {})
                title = str(update.get("title") or "").strip()
                tool_kind = str(update.get("kind") or "").strip()
                status = str(update.get("status") or "").strip()
                if title:
                    task["title"] = title
                if tool_kind:
                    task["kind"] = tool_kind
                if status:
                    task["status"] = status
                if status.lower() in {"completed", "failed", "cancelled", "canceled", "error"}:
                    self._active_tools.pop(tool_call_id, None)
            await self.on_event("tool", {
                "title": str(update.get("title") or "tool"),
                "kind": str(update.get("kind") or ""),
                "status": str(update.get("status") or ""),
            })
        elif kind == "plan":
            await self.on_event("plan", {"entries": update.get("entries") or []})

    # ------------------------------------------------------ server -> client

    async def _handle_server_request(self, msg: dict[str, Any]) -> None:
        method, message_id, params = msg["method"], msg.get("id"), msg.get("params") or {}
        try:
            if method == "session/request_permission":
                option_id = await self.permission_handler(params)
                if option_id:
                    result: Any = {"outcome": {"outcome": "selected", "optionId": option_id}}
                else:
                    result = {"outcome": {"outcome": "cancelled"}}
                await self._respond(message_id, result=result)
            elif method == "fs/read_text_file":
                await self._respond(message_id, result=self._fs_read(params))
            elif method == "fs/write_text_file":
                await self._respond(message_id, result=self._fs_write(params))
            else:
                await self._respond(message_id, error=(-32601, f"Unsupported ACP client method: {method}"))
        except Exception as exc:
            logger.exception("handling ACP server request %s failed", method)
            await self._respond(message_id, error=(-32602, str(exc)))

    async def _respond(self, message_id: Any, *, result: Any = None, error: tuple[int, str] | None = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": message_id}
        if error is not None:
            payload["error"] = {"code": error[0], "message": error[1]}
        else:
            payload["result"] = result
        try:
            await self._write(payload)
        except Exception:
            logger.exception("failed to respond to server request id=%s", message_id)

    def _fs_read(self, params: dict[str, Any]) -> dict[str, Any]:
        path = _ensure_within_cwd(str(params.get("path") or ""), self.cwd)
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            content = ""
        line, limit = params.get("line"), params.get("limit")
        if isinstance(line, int) and line > 1:
            end = line - 1 + limit if isinstance(limit, int) and limit > 0 else None
            content = "".join(content.splitlines(keepends=True)[line - 1:end])
        return {"content": content}

    def _fs_write(self, params: dict[str, Any]) -> None:
        path = _ensure_within_cwd(str(params.get("path") or ""), self.cwd)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(params.get("content") or ""), encoding="utf-8")
        return None
