"""Tests for bundle (batch) dispatching in the dispatcher.

Uses nested bundles and a virtual clock to verify batch boundaries, dispatch
order and the defined outcomes for bad packets, unknown addresses, handler
exceptions, cancellation, empty bundles and oversized batches.
"""

import unittest

from pythonosc.dispatcher import BatchMessage, Dispatcher
from pythonosc.osc_bundle_builder import IMMEDIATELY, OscBundleBuilder
from pythonosc.osc_message_builder import OscMessageBuilder


class FakeClock:
    """Virtual clock: sleeping advances time instead of blocking."""

    def __init__(self, now=1000.0):
        self.now = now
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def build_message(address, *args):
    builder = OscMessageBuilder(address)
    for arg in args:
        builder.add_arg(arg)
    return builder.build()


def build_bundle(timestamp, *contents):
    builder = OscBundleBuilder(timestamp)
    for content in contents:
        builder.add_content(content)
    return builder.build()


def lighting_bundle_dgram():
    """A lighting desk sending color temperature, brightness and hue at once."""
    return build_bundle(
        IMMEDIATELY,
        build_message("/light/ct", 3200),
        build_message("/light/brightness", 0.5),
        build_message("/light/hue", 180),
    ).dgram


class TestBundleDispatch(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.dispatcher = Dispatcher(
            time_func=self.clock.time, sleep_func=self.clock.sleep
        )
        self.client = ("127.0.0.1", 7777)

    def test_batch_handler_receives_whole_bundle_at_once(self):
        batches = []
        self.dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))

        results = self.dispatcher.call_handlers_for_packet(
            lighting_bundle_dgram(), self.client
        )

        self.assertEqual([], results)
        self.assertEqual(1, len(batches))
        self.assertEqual(
            [
                BatchMessage("/light/ct", (3200,)),
                BatchMessage("/light/brightness", (0.5,)),
                BatchMessage("/light/hue", (180,)),
            ],
            batches[0],
        )

    def test_batch_handler_with_fixed_args_and_reply_address(self):
        seen = []
        self.dispatcher.map_bundle(
            "/light/*",
            lambda client, messages, fixed: seen.append((client, messages, fixed)),
            "fixture",
            needs_reply_address=True,
        )

        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)

        self.assertEqual(1, len(seen))
        client, messages, fixed = seen[0]
        self.assertEqual(self.client, client)
        self.assertEqual(3, len(messages))
        self.assertEqual(["fixture"], fixed)

    def test_batch_handler_reply_is_returned(self):
        self.dispatcher.map_bundle("/light/*", lambda messages: ("/ack", 1))

        results = self.dispatcher.call_handlers_for_packet(
            lighting_bundle_dgram(), self.client
        )

        self.assertEqual([("/ack", 1)], results)

    def test_nested_bundles_grouped_by_their_own_timetag(self):
        # Outer bundle fires immediately, nested bundles at now+5 and now+10.
        dgram = build_bundle(
            IMMEDIATELY,
            build_message("/a", 1),
            build_bundle(
                self.clock.now + 5,
                build_message("/b", 2),
                build_message("/c", 3),
            ),
            build_bundle(
                self.clock.now + 10,
                build_message("/d", 4),
            ),
        ).dgram

        batches = []
        self.dispatcher.map_bundle("/*", lambda messages: batches.append(messages))

        self.dispatcher.call_handlers_for_packet(dgram, self.client)

        # One delivery per effective timetag, in time order, order preserved.
        self.assertEqual(
            [["/a"], ["/b", "/c"], ["/d"]],
            [[m.address for m in batch] for batch in batches],
        )
        # The virtual clock only advanced for the two future groups.
        self.assertAlmostEqual(10.0, sum(self.clock.sleeps))
        self.assertGreaterEqual(self.clock.now, 1000.0 + 10)

    def test_past_nested_bundle_joins_immediate_group(self):
        dgram = build_bundle(
            IMMEDIATELY,
            build_message("/a", 1),
            build_bundle(
                self.clock.now - 5,  # already due: effective timetag is "now"
                build_message("/b", 2),
            ),
        ).dgram

        batches = []
        self.dispatcher.map_bundle("/*", lambda messages: batches.append(messages))

        self.dispatcher.call_handlers_for_packet(dgram, self.client)

        self.assertEqual([["/a", "/b"]], [[m.address for m in b] for b in batches])
        self.assertEqual([], self.clock.sleeps)

    def test_plain_handlers_keep_per_message_semantics(self):
        plain_calls = []
        batch_calls = []
        self.dispatcher.map("/light/*", lambda address, *args: plain_calls.append(address))
        self.dispatcher.map_bundle(
            "/light/*", lambda messages: batch_calls.append(messages)
        )

        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)

        # Plain handlers are still invoked once per message, in order...
        self.assertEqual(
            ["/light/ct", "/light/brightness", "/light/hue"], plain_calls
        )
        # ...while the batch handler gets a single delivery.
        self.assertEqual(1, len(batch_calls))
        self.assertEqual(3, len(batch_calls[0]))

    def test_bad_packet_calls_nothing_and_returns_empty(self):
        plain_calls = []
        batch_calls = []
        self.dispatcher.map("/light/*", lambda *args: plain_calls.append(args))
        self.dispatcher.map_bundle(
            "/light/*", lambda messages: batch_calls.append(messages)
        )

        for garbage in (b"", b"not an osc packet", b"#bundle\x00\x00\x01"):
            results = self.dispatcher.call_handlers_for_packet(garbage, self.client)
            self.assertEqual([], results)

        self.assertEqual([], plain_calls)
        self.assertEqual([], batch_calls)

    def test_unknown_address_calls_nothing(self):
        batch_calls = []
        self.dispatcher.map_bundle("/other/*", lambda messages: batch_calls.append(messages))

        results = self.dispatcher.call_handlers_for_packet(
            lighting_bundle_dgram(), self.client
        )

        self.assertEqual([], results)
        self.assertEqual([], batch_calls)

    def test_unknown_address_still_reaches_default_handler(self):
        default_calls = []
        batch_calls = []
        self.dispatcher.set_default_handler(
            lambda address, *args: default_calls.append(address)
        )
        self.dispatcher.map_bundle("/other/*", lambda messages: batch_calls.append(messages))

        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)

        self.assertEqual(
            ["/light/ct", "/light/brightness", "/light/hue"], default_calls
        )
        self.assertEqual([], batch_calls)

    def test_bundle_handler_exception_does_not_stop_others(self):
        batches = []

        def failing(messages):
            raise RuntimeError("boom")

        self.dispatcher.map_bundle("/light/*", failing)
        self.dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))

        with self.assertLogs("pythonosc.dispatcher", level="ERROR"):
            results = self.dispatcher.call_handlers_for_packet(
                lighting_bundle_dgram(), self.client
            )

        self.assertEqual([], results)
        self.assertEqual(1, len(batches))
        self.assertEqual(3, len(batches[0]))

    def test_future_timetag_cancellation(self):
        dgram = build_bundle(
            self.clock.now + 60,
            build_message("/light/ct", 3200),
        ).dgram

        batches = []
        self.dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))

        clock = self.clock
        dispatcher = self.dispatcher

        def cancelling_sleep(seconds):
            clock.sleep(seconds)
            dispatcher.cancel_pending()

        self.dispatcher._sleep_func = cancelling_sleep

        results = self.dispatcher.call_handlers_for_packet(dgram, self.client)

        self.assertEqual([], results)
        self.assertEqual([], batches)
        # Cancellation stays in effect until explicitly cleared.
        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)
        self.assertEqual([], batches)
        self.dispatcher.clear_cancellation()
        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)
        self.assertEqual(1, len(batches))

    def test_empty_bundle_calls_nothing(self):
        batches = []
        self.dispatcher.map_bundle("/*", lambda messages: batches.append(messages))

        results = self.dispatcher.call_handlers_for_packet(
            build_bundle(IMMEDIATELY).dgram, self.client
        )

        self.assertEqual([], results)
        self.assertEqual([], batches)

    def test_oversized_batch_is_chunked_in_order(self):
        dispatcher = Dispatcher(
            max_batch_size=2,
            time_func=self.clock.time,
            sleep_func=self.clock.sleep,
        )
        dgram = build_bundle(
            IMMEDIATELY,
            build_message("/light/a", 1),
            build_message("/light/b", 2),
            build_message("/light/c", 3),
            build_message("/light/d", 4),
            build_message("/light/e", 5),
        ).dgram

        batches = []
        dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))

        dispatcher.call_handlers_for_packet(dgram, self.client)

        self.assertEqual([2, 2, 1], [len(batch) for batch in batches])
        self.assertEqual(
            [1, 2, 3, 4, 5],
            [message.args[0] for batch in batches for message in batch],
        )

    def test_invalid_max_batch_size_raises(self):
        with self.assertRaises(ValueError):
            Dispatcher(max_batch_size=0)

    def test_unmap_bundle(self):
        batches = []
        handler = self.dispatcher.map_bundle(
            "/light/*", lambda messages: batches.append(messages)
        )
        self.dispatcher.unmap_bundle("/light/*", handler)

        self.dispatcher.call_handlers_for_packet(lighting_bundle_dgram(), self.client)
        self.assertEqual([], batches)

        with self.assertRaises(ValueError):
            self.dispatcher.unmap_bundle("/light/*", handler)


class TestAsyncBundleDispatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.dispatcher = Dispatcher(
            time_func=self.clock.time, sleep_func=self.clock.sleep
        )
        self.client = ("127.0.0.1", 7777)

    async def test_async_batch_dispatch_sync_and_async_handlers(self):
        async_batches = []
        sync_batches = []

        async def async_handler(messages):
            async_batches.append(messages)

        self.dispatcher.map_bundle("/light/*", async_handler)
        self.dispatcher.map_bundle("/light/*", lambda messages: sync_batches.append(messages))

        results = await self.dispatcher.async_call_handlers_for_packet(
            lighting_bundle_dgram(), self.client
        )

        self.assertEqual([], results)
        self.assertEqual(1, len(async_batches))
        self.assertEqual(1, len(sync_batches))
        self.assertEqual(
            ["/light/ct", "/light/brightness", "/light/hue"],
            [m.address for m in async_batches[0]],
        )

    async def test_async_cancellation_skips_future_group(self):
        dgram = build_bundle(
            self.clock.now + 60,
            build_message("/light/ct", 3200),
        ).dgram

        batches = []
        self.dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))
        self.dispatcher.cancel_pending()

        results = await self.dispatcher.async_call_handlers_for_packet(dgram, self.client)

        self.assertEqual([], results)
        self.assertEqual([], batches)

    async def test_async_bundle_handler_exception_does_not_stop_others(self):
        batches = []

        async def failing(messages):
            raise RuntimeError("boom")

        self.dispatcher.map_bundle("/light/*", failing)
        self.dispatcher.map_bundle("/light/*", lambda messages: batches.append(messages))

        with self.assertLogs("pythonosc.dispatcher", level="ERROR"):
            results = await self.dispatcher.async_call_handlers_for_packet(
                lighting_bundle_dgram(), self.client
            )

        self.assertEqual([], results)
        self.assertEqual(1, len(batches))


if __name__ == "__main__":
    unittest.main()
