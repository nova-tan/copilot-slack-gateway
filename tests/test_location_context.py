import unittest
from types import SimpleNamespace

from copilot_gateway.slack_app import location_context, stream_kickoff_prompt


class LocationContextTests(unittest.TestCase):
    def test_dm_is_labelled_with_id(self) -> None:
        conv = SimpleNamespace(channel="D123", thread_ts=None)

        text = location_context("https://moltygroup.slack.com", conv)

        self.assertIn("https://moltygroup.slack.com", text)
        self.assertIn("DM (D123)", text)
        self.assertNotIn("thread", text)

    def test_stream_channel_includes_name_and_thread_when_present(self) -> None:
        conv = SimpleNamespace(channel="C123", thread_ts="1700000000.000001")

        text = location_context("https://moltygroup.slack.com", conv, "stream-foo")

        self.assertIn("channel C123 (#stream-foo)", text)
        self.assertIn("thread 1700000000.000001", text)

    def test_warns_against_skill_defaults(self) -> None:
        conv = SimpleNamespace(channel="C123", thread_ts=None)

        text = location_context("https://moltygroup.slack.com", conv)

        self.assertIn("not any default", text)


class StreamKickoffPromptTests(unittest.TestCase):
    def test_kickoff_identifies_channel_and_workspace(self) -> None:
        prompt = stream_kickoff_prompt(
            "my-stream",
            "stream-my-stream",
            "do the thing",
            channel_id="C123",
            workspace_url="https://moltygroup.slack.com",
        )

        self.assertIn("channel `C123`", prompt)
        self.assertIn("workspace https://moltygroup.slack.com", prompt)
        self.assertIn("#stream-my-stream", prompt)


if __name__ == "__main__":
    unittest.main()
