import asyncio
import tempfile
import unittest
from pathlib import Path

from copilot_gateway.config import Config
from copilot_gateway.sessions import SessionRegistry
from copilot_gateway.slack_app import steer_prompt


class _StubSession:
    def __init__(self, busy: bool) -> None:
        self._busy = busy
        self.cancel_calls = 0

    def busy(self) -> bool:
        return self._busy

    async def cancel(self) -> None:
        self.cancel_calls += 1


def _registry(directory: str) -> SessionRegistry:
    return SessionRegistry(Config(
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        state_dir=Path(directory),
        cwd=directory,
    ))


class SteerPromptTests(unittest.TestCase):
    def test_steer_prompt_marks_interruption(self) -> None:
        prompt = steer_prompt("use pytest instead")

        self.assertIn("interrupted", prompt)
        self.assertTrue(prompt.endswith("use pytest instead"))


class SteerTests(unittest.IsolatedAsyncioTestCase):
    async def test_steer_without_conversation_returns_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            registry = _registry(directory)

            self.assertFalse(await registry.steer("D123", "more info"))

    async def test_steer_with_idle_session_returns_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            registry = _registry(directory)
            conv, _ = await registry.get_or_create(
                "D123",
                channel="D123",
                thread_ts=None,
                on_event_factory=lambda conv: None,
                permission_handler_factory=lambda conv: None,
            )
            conv.session = _StubSession(busy=False)

            self.assertFalse(await registry.steer("D123", "more info"))

    async def test_steer_cancels_and_jumps_the_queue(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            registry = _registry(directory)
            conv, _ = await registry.get_or_create(
                "D123",
                channel="D123",
                thread_ts=None,
                on_event_factory=lambda conv: None,
                permission_handler_factory=lambda conv: None,
            )
            session = _StubSession(busy=True)
            conv.session = session
            conv.queue.put_nowait("earlier one")
            conv.queue.put_nowait("earlier two")

            self.assertTrue(await registry.steer("D123", "steering info"))

            self.assertEqual(session.cancel_calls, 1)
            self.assertEqual(
                [conv.queue.get_nowait() for _ in range(3)],
                ["steering info", "earlier one", "earlier two"],
            )


if __name__ == "__main__":
    unittest.main()
