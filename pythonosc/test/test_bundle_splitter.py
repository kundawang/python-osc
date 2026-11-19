import time
import unittest
from typing import Union

from pythonosc import bundle_splitter
from pythonosc import osc_bundle
from pythonosc import osc_bundle_builder
from pythonosc import osc_message
from pythonosc import osc_message_builder
from pythonosc import osc_packet
from pythonosc.parsing import osc_types

# A whole-second timestamp in the future: integer seconds round-trip exactly
# through the NTP timetag, and a future timetag is not rewritten to "now" by
# OscPacket when the packet is parsed.
TIMESTAMP = int(time.time()) + 3600


def _make_message(index: int) -> osc_message.OscMessage:
    builder = osc_message_builder.OscMessageBuilder(address=f"/test/{index}")
    builder.add_arg(index)
    return builder.build()


def _make_bundle(
    num_messages: int, timestamp: Union[int, float] = TIMESTAMP
) -> osc_bundle.OscBundle:
    builder = osc_bundle_builder.OscBundleBuilder(timestamp)
    for index in range(num_messages):
        builder.add_content(_make_message(index))
    return builder.build()


class TestBoundedPacking(unittest.TestCase):
    def test_single_bundle_when_everything_fits(self):
        bundle = _make_bundle(3)
        fragments = bundle_splitter.split_bundle(bundle, bundle.size)
        self.assertEqual(1, len(fragments))
        self.assertEqual(bundle.dgram, fragments[0].dgram)

    def test_splits_100_messages_into_ordered_bounded_bundles(self):
        num_messages = 100
        mtu = 108
        bundle = _make_bundle(num_messages)
        fragments = bundle_splitter.split_bundle(bundle, mtu)

        # More than one datagram is needed, none exceeds the MTU, and every
        # datagram is a legal bundle parsable by the existing parser.
        self.assertGreater(len(fragments), 1)
        seen = []
        for fragment in fragments:
            self.assertLessEqual(fragment.size, mtu)
            self.assertEqual(TIMESTAMP, fragment.timestamp)
            parsed = osc_bundle.OscBundle(fragment.dgram)
            self.assertEqual(parsed.dgram, fragment.dgram)
            for message in parsed:
                self.assertEqual(osc_message.OscMessage, type(message))
                seen.append(message.address)

        # Order and completeness: all 100 messages, in original order.
        self.assertEqual(num_messages, len(seen))
        self.assertEqual([f"/test/{index}" for index in range(num_messages)], seen)

    def test_packs_as_many_elements_as_fit_per_datagram(self):
        # Use identical messages so each size-prefixed element has one size.
        message = _make_message(0)
        element_size = 4 + message.size
        builder = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        for _ in range(5):
            builder.add_content(_make_message(0))
        bundle = builder.build()
        # An MTU that holds exactly two elements but not three.
        mtu = bundle_splitter.BUNDLE_HEADER_SIZE + 2 * element_size
        fragments = bundle_splitter.split_bundle(bundle, mtu)
        # 5 uniform messages, 2 per bundle -> bundles of 2, 2, 1.
        self.assertEqual([2, 2, 1], [f.num_contents for f in fragments])
        # Every full bundle reaches the MTU, proving greedy packing.
        for fragment in fragments[:-1]:
            self.assertEqual(mtu, fragment.size)

    def test_greedy_boundary_respects_size_not_count(self):
        builder = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        small = _make_message(0)
        big_builder = osc_message_builder.OscMessageBuilder(address="/big")
        big_builder.add_arg("x" * 32)
        big = big_builder.build()
        builder.add_content(small)
        builder.add_content(big)
        bundle = builder.build()

        mtu = bundle_splitter.BUNDLE_HEADER_SIZE + 4 + big.size
        fragments = bundle_splitter.split_bundle(bundle, mtu)
        # The big message alone fills a bundle; the small one gets its own.
        self.assertEqual(2, len(fragments))
        self.assertEqual(1, fragments[0].num_contents)
        self.assertEqual(1, fragments[1].num_contents)
        self.assertEqual(b"/big" + b"\x00" * 4, fragments[1].content(0).dgram[:8])

    def test_preserves_nested_bundle_semantics_when_splitting(self):
        inner = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        inner.add_content(_make_message(0))
        inner_bundle = inner.build()

        outer = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        outer.add_content(inner_bundle)
        outer.add_content(_make_message(1))
        outer_bundle = outer.build()

        mtu = bundle_splitter.BUNDLE_HEADER_SIZE + 4 + inner_bundle.size
        fragments = bundle_splitter.split_bundle(outer_bundle, mtu)
        self.assertEqual(2, len(fragments))
        # The nested bundle travels whole, in first position.
        first = osc_bundle.OscBundle(fragments[0].dgram)
        self.assertEqual(osc_bundle.OscBundle, type(first.content(0)))
        self.assertEqual(inner_bundle.dgram, first.content(0).dgram)
        self.assertEqual("/test/1", fragments[1].content(0).address)

    def test_nested_bundle_keeps_its_own_timetag_when_splitting(self):
        # Legal per the OSC spec: a nested bundle may carry its own timetag.
        inner = osc_bundle_builder.OscBundleBuilder(1_600_000_000.0)
        inner.add_content(_make_message(0))
        inner_bundle = inner.build()

        outer = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        outer.add_content(inner_bundle)
        outer_bundle = outer.build()

        fragments = bundle_splitter.split_bundle(outer_bundle, outer_bundle.size)
        self.assertEqual(1, len(fragments))
        self.assertEqual(1_600_000_000.0, fragments[0].content(0).timestamp)

    def test_every_fragment_round_trips_through_osc_packet(self):
        bundle = _make_bundle(100)
        fragments = bundle_splitter.split_bundle(bundle, 108)
        total = 0
        for fragment in fragments:
            packet = osc_packet.OscPacket(fragment.dgram)
            total += len(packet.messages)
            for timed_message in packet.messages:
                self.assertEqual(TIMESTAMP, timed_message.time)
        self.assertEqual(100, total)

    def test_immediately_timetag_is_preserved(self):
        bundle = _make_bundle(4, timestamp=osc_types.IMMEDIATELY)
        fragments = bundle_splitter.split_bundle(bundle, 108)
        for fragment in fragments:
            self.assertEqual(osc_types.IMMEDIATELY, fragment.timestamp)


class TestBoundedBuilder(unittest.TestCase):
    def test_builds_bundles_from_contents(self):
        messages = [_make_message(i) for i in range(4)]
        fragments = bundle_splitter.build_bounded_bundles(messages, TIMESTAMP, 108)
        self.assertTrue(all(f.timestamp == TIMESTAMP for f in fragments))
        self.assertEqual(
            [m.address for m in messages],
            [msg.address for f in fragments for msg in f],
        )

    def test_rejects_nested_bundle_with_mismatched_timetag(self):
        inner = osc_bundle_builder.OscBundleBuilder(1_600_000_000.0)
        inner.add_content(_make_message(0))
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.build_bounded_bundles([inner.build()], TIMESTAMP, 512)

    def test_accepts_nested_bundle_with_matching_timetag(self):
        inner = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        inner.add_content(_make_message(0))
        fragments = bundle_splitter.build_bounded_bundles(
            [inner.build()], TIMESTAMP, 512
        )
        self.assertEqual(1, len(fragments))


class TestBundlingErrors(unittest.TestCase):
    def test_rejects_limit_smaller_than_header(self):
        bundle = _make_bundle(1)
        for bad_limit in (-1, 0, 15):
            with self.subTest(bad_limit=bad_limit):
                with self.assertRaises(bundle_splitter.BundleBuildError):
                    bundle_splitter.split_bundle(bundle, bad_limit)

    def test_rejects_non_integer_limit(self):
        bundle = _make_bundle(1)
        for bad_limit in (128.0, "128", True):
            with self.subTest(bad_limit=bad_limit):
                with self.assertRaises(bundle_splitter.BundleBuildError):
                    bundle_splitter.split_bundle(bundle, bad_limit)

    def test_rejects_empty_bundle(self):
        empty = osc_bundle_builder.OscBundleBuilder(TIMESTAMP).build()
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.split_bundle(empty, 512)

    def test_rejects_empty_contents(self):
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.build_bounded_bundles([], TIMESTAMP, 512)

    def test_rejects_single_message_that_exceeds_limit(self):
        message = _make_message(0)
        # Large enough for the header but not for one size-prefixed message.
        too_small = bundle_splitter.BUNDLE_HEADER_SIZE + 4 + message.size - 1
        bundle = _make_bundle(1)
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.split_bundle(bundle, too_small)

    def test_rejects_oversized_nested_bundle(self):
        inner = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        inner.add_content(_make_message(0))
        inner_bundle = inner.build()
        outer = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
        outer.add_content(inner_bundle)
        bundle = outer.build()
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.split_bundle(bundle, bundle_splitter.BUNDLE_HEADER_SIZE)

    def test_rejects_non_bundle_argument(self):
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.split_bundle("not a bundle", 512)

    def test_rejects_invalid_content_type(self):
        with self.assertRaises(bundle_splitter.BundleBuildError):
            bundle_splitter.build_bounded_bundles([None], TIMESTAMP, 512)

    def test_failure_sends_nothing_and_builds_no_partial_bundles(self):
        # The oversized element comes after valid ones; validation must fail
        # up front rather than return a partial fragment list.
        big_builder = osc_message_builder.OscMessageBuilder(address="/big")
        big_builder.add_arg("z" * 200)
        contents = [_make_message(0), big_builder.build()]
        with self.assertRaises(bundle_splitter.BundleBuildError) as context:
            bundle_splitter.build_bounded_bundles(contents, TIMESTAMP, 128)
        # Every build failure reports an explicit zero transmitted count.
        self.assertEqual(0, context.exception.sent_count)


if __name__ == "__main__":
    unittest.main()
