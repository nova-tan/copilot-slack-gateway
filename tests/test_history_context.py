import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from copilot_gateway.config import Config, _non_negative_int
from copilot_gateway.sessions import SessionRegistry
from copilot_gateway.slack_app import (
    CONTEXT_MESSAGE_LIMIT,
    HistoryFetcher,
    format_history_context,
)

ALLOWED = frozenset({"U1", "U2"})


def _msg(ts, text, user="U1", **extra):
    msg = {"ts": ts, "text": text, "user": user}
    msg.update(extra)
    return msg


class FormatHistoryContextTests(unittest.TestCase):
    def test_empty_after_filtering_returns_none(self) -> None:
        self.assertIsNone(format_history_context([], bot_user_id="UBOT", allowed_users=ALLOWED))
        self.assertIsNone(format_history_context(
            [_msg("1.0", "hello", user="USTRANGER")], bot_user_id="UBOT", allowed_users=ALLOWED))

    def test_output_is_oldest_first_regardless_of_api_order(self) -> None:
        messages = [_msg("3.0", "third"), _msg("1.0", "first"), _msg("2.0", "second")]

        context = format_history_context(messages, bot_user_id="UBOT", allowed_users=ALLOWED)

        self.assertLess(context.index("first"), context.index("second"))
        self.assertLess(context.index("second"), context.index("third"))

    def test_skips_subtypes_status_lines_and_unknown_bots(self) -> None:
        messages = [
            _msg("1.0", "has joined the channel", subtype="channel_join"),
            _msg("2.0", "⏳ Working…", user="UBOT", bot_id="B1"),
            _msg("3.0", "🟢 Copilot session started (`abc`)", user="UBOT", bot_id="B1"),
            _msg("4.0", "unrelated integration noise", user="UOTHER", bot_id="BOTHER"),
            _msg("5.0", "real question"),
        ]

        context = format_history_context(messages, bot_user_id="UBOT", allowed_users=ALLOWED)

        self.assertIn("real question", context)
        for dropped in ("joined the channel", "Working", "session started", "noise"):
            self.assertNotIn(dropped, context)

    def test_own_bot_messages_are_labelled_as_gateway(self) -> None:
        context = format_history_context(
            [_msg("1.0", "previous answer", user="UBOT", bot_id="B1")],
            bot_user_id="UBOT",
            allowed_users=ALLOWED,
        )

        self.assertIn("copilot-gateway: previous answer", context)

    def test_long_message_is_truncated(self) -> None:
        context = format_history_context(
            [_msg("1.0", "x" * (CONTEXT_MESSAGE_LIMIT + 50))],
            bot_user_id="UBOT",
            allowed_users=ALLOWED,
        )

        self.assertIn("x" * CONTEXT_MESSAGE_LIMIT + "…", context)
        self.assertNotIn("x" * (CONTEXT_MESSAGE_LIMIT + 1), context)

    def test_total_cap_keeps_newest_messages(self) -> None:
        messages = [
            _msg(f"{i}.0", f"message-{i} " + "y" * 300) for i in range(40)
        ]
        with mock.patch("copilot_gateway.slack_app.CONTEXT_TOTAL_LIMIT", 1200):
            context = format_history_context(messages, bot_user_id="UBOT", allowed_users=ALLOWED)

        self.assertIn("message-39", context)
        self.assertNotIn("message-0", context)


class _StubClient:
    def __init__(self, messages=(), error=None) -> None:
        self.messages = list(messages)
        self.error = error
        self.calls = []

    async def auth_test(self):
        return {"user_id": "UBOT"}

    async def conversations_history(self, **kwargs):
        self.calls.append(("history", kwargs))
        if self.error:
            raise self.error
        return {"messages": self.messages}

    async def conversations_replies(self, **kwargs):
        self.calls.append(("replies", kwargs))
        if self.error:
            raise self.error
        return {"messages": self.messages}


def _config(directory: str, history_messages: int = 30) -> Config:
    return Config(
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        state_dir=Path(directory),
        cwd=directory,
        allowed_users=ALLOWED,
        context_history_messages=history_messages,
    )


class HistoryFetcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_config_never_calls_slack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _StubClient(messages=[_msg("1.0", "hi")])
            fetcher = HistoryFetcher(client, _config(directory, history_messages=0))

            result = await fetcher.recent_context(SimpleNamespace(channel="D1", thread_ts=None, key="D1"))

            self.assertIsNone(result)
            self.assertEqual(client.calls, [])

    async def test_fetch_failure_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _StubClient(error=RuntimeError("boom"))
            fetcher = HistoryFetcher(client, _config(directory))

            result = await fetcher.recent_context(SimpleNamespace(channel="D1", thread_ts=None, key="D1"))

            self.assertIsNone(result)

    async def test_dm_uses_history_and_thread_uses_replies(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _StubClient(messages=[_msg("1.0", "hi")])
            fetcher = HistoryFetcher(client, _config(directory))

            await fetcher.recent_context(SimpleNamespace(channel="D1", thread_ts=None, key="D1"))
            await fetcher.recent_context(SimpleNamespace(channel="C1", thread_ts="9.9", key="C1:9.9"))

            self.assertEqual(client.calls[0][0], "history")
            self.assertEqual(client.calls[1][0], "replies")
            self.assertEqual(client.calls[1][1]["ts"], "9.9")

    async def test_thread_replies_oldest_first_are_normalized(self) -> None:
        # conversations.replies returns the parent first, chronologically.
        messages = [_msg(f"{i}.0", f"reply-{i}") for i in range(10)]
        with tempfile.TemporaryDirectory() as directory:
            client = _StubClient(messages=messages)
            fetcher = HistoryFetcher(client, _config(directory))

            with mock.patch("copilot_gateway.slack_app.CONTEXT_TOTAL_LIMIT", 120):
                context = await fetcher.recent_context(
                    SimpleNamespace(channel="C1", thread_ts="0.0", key="C1:0.0"))

            self.assertIn("reply-9", context)
            self.assertNotIn("reply-0", context)


class SkipHistoryFlagTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_command_marks_next_start_to_skip_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            registry = SessionRegistry(_config(directory))
            conv, _ = await registry.get_or_create(
                "D1",
                channel="D1",
                thread_ts=None,
                on_event_factory=lambda conv: None,
                permission_handler_factory=lambda conv: None,
            )

            self.assertFalse(conv.skip_history_on_next_start)
            self.assertTrue(await registry.reset("D1"))
            self.assertTrue(registry.get_conversation("D1").skip_history_on_next_start)


class ConfigParsingTests(unittest.TestCase):
    def test_rejects_negative_and_non_integer_values(self) -> None:
        for raw in ("-1", "abc", "1.5"):
            with self.assertRaises(SystemExit):
                _non_negative_int(raw, "CONTEXT_HISTORY_MESSAGES")

    def test_clamps_to_slack_limit(self) -> None:
        self.assertEqual(_non_negative_int("5000", "CONTEXT_HISTORY_MESSAGES"), 1000)


if __name__ == "__main__":
    unittest.main()
