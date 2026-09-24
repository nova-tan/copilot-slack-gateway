import tempfile
import unittest
from pathlib import Path

from copilot_gateway.config import Config
from copilot_gateway.sessions import SessionRegistry
from copilot_gateway.slack_app import (
    provisional_stream_id,
    stream_channel_name,
    stream_id_from_text,
)


class StreamChannelTests(unittest.TestCase):
    def test_issue_url_has_stable_stream_id_and_channel_name(self) -> None:
        stream_id = stream_id_from_text("https://github.com/acme/widget/issues/42")

        self.assertEqual(stream_id, "acme-widget-issue-42")
        self.assertEqual(stream_channel_name(stream_id), "stream-acme-widget-issue-42")

    def test_long_description_keeps_channel_name_within_slack_limit(self) -> None:
        stream_id = stream_id_from_text("A " * 100)

        self.assertLessEqual(len(stream_channel_name(stream_id)), 80)
        self.assertRegex(stream_id, r"-[0-9a-f]{8}$")

    def test_bare_command_can_use_a_provisional_stream_id(self) -> None:
        stream_id = provisional_stream_id()

        self.assertRegex(stream_id, r"^new-\d{8}-\d{6}-[0-9a-f]{6}$")
        self.assertLessEqual(len(stream_channel_name(stream_id)), 80)

    def test_stream_channel_mapping_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = Config(
                slack_bot_token="xoxb-test",
                slack_app_token="xapp-test",
                state_dir=Path(directory),
                cwd=directory,
            )
            registry = SessionRegistry(config)
            registry.register_stream_channel(
                stream_id="acme-widget-issue-42",
                channel_id="C123",
                name="stream-acme-widget-issue-42",
                description="https://github.com/acme/widget/issues/42",
                created_by="U123",
            )

            restored = SessionRegistry(config)

            self.assertTrue(restored.is_stream_channel("C123"))
            self.assertEqual(
                restored.get_stream_channel("acme-widget-issue-42")["channel_id"],
                "C123",
            )

    def test_stream_channel_uses_one_conversation_key(self) -> None:
        self.assertEqual(
            SessionRegistry.key_for("C123", "1710000000.000001", is_stream=True),
            "C123",
        )


if __name__ == "__main__":
    unittest.main()
