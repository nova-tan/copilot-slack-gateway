"""Smoke test: persistent ACPSession against a real `copilot --acp --stdio` process.

Verifies (no Slack needed):
  1. process spawn + initialize + session/new
  2. first prompt streams text chunks
  3. SECOND prompt reuses the SAME session (multi-turn memory)
  4. session survives across prompts (the whole point of the gateway)

Run:  python scripts/acp_smoke.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from copilot_gateway.acp import ACPSession  # noqa: E402


async def main() -> int:
    events: list[tuple[str, str]] = []

    async def on_event(kind: str, data: dict) -> None:
        if kind == "text":
            events.append((kind, data["text"]))
            print(data["text"], end="", flush=True)
        elif kind == "tool":
            print(f"\n[tool] {data['title']} — {data['status']}")
        elif kind == "error":
            print(f"\n[error] {data['message']}")

    async def on_permission(params: dict) -> str | None:
        print(f"\n[permission requested] {params.get('toolCall', {}).get('title')} -> auto-deny in smoke test")
        return None

    session = ACPSession(
        command="copilot",
        args=("--acp", "--stdio"),
        cwd=str(Path(os.environ.get("GATEWAY_CWD", str(Path.home()))).expanduser().resolve()),
        model=None,
        on_event=on_event,
        permission_handler=on_permission,
    )

    print("== starting session ==")
    await session.start()
    print(f"session_id={session.session_id} pid={session.pid}")
    assert session.alive

    print("\n== prompt 1 ==")
    await session.prompt("Remember the codeword PINEAPPLE-42. Reply with just: acknowledged")
    first_session = session.session_id

    print("\n\n== prompt 2 (same process, tests multi-turn memory) ==")
    await session.prompt("What is the codeword? Reply with just the codeword.")

    assert session.alive, "session should still be alive after two prompts"
    assert session.session_id == first_session, "session id must be stable across prompts"

    text = "".join(t for k, t in events if k == "text")
    ok = "PINEAPPLE-42" in text
    print(f"\n\n== result: {'PASS' if ok else 'FAIL'} (codeword recalled: {ok}) ==")
    await session.close()
    assert not session.alive
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
