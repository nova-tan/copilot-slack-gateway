import unittest

from copilot_gateway.slack_app import StreamRenderer


class _StubClient:
    def __init__(self) -> None:
        self.posts = []
        self.updates = []

    async def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ts": "123.456"}

    async def chat_update(self, **kwargs):
        self.updates.append(kwargs)


def _cancel_button(blocks):
    return blocks[0]["elements"][0]


class CancelButtonTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_posts_cancel_button_with_conversation_key(self) -> None:
        client = _StubClient()
        renderer = StreamRenderer(client, "C1", None, False, conv_key="C1:1700000000.000001")

        await renderer.start()

        button = _cancel_button(client.posts[0]["blocks"])
        self.assertEqual(button["action_id"], "cancel_turn")
        self.assertEqual(button["value"], "C1:1700000000.000001")
        self.assertEqual(button["style"], "danger")

    async def test_live_updates_keep_the_button(self) -> None:
        client = _StubClient()
        renderer = StreamRenderer(client, "C1", None, False, conv_key="C1")
        await renderer.start()

        await renderer.add_text("partial output")
        await renderer._maybe_update(force=True)

        self.assertEqual(_cancel_button(client.updates[-1]["blocks"])["action_id"], "cancel_turn")

    async def test_finalize_strips_the_button(self) -> None:
        client = _StubClient()
        renderer = StreamRenderer(client, "C1", None, False, conv_key="C1")
        await renderer.start()
        await renderer.add_text("done")

        await renderer.finalize()

        self.assertEqual(client.updates[-1]["blocks"], [])


if __name__ == "__main__":
    unittest.main()
