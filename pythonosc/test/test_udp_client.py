import socket
import time
import unittest
from unittest import mock

from pythonosc import bundle_splitter
from pythonosc import osc_bundle
from pythonosc import osc_bundle_builder
from pythonosc import osc_message_builder
from pythonosc import osc_packet
from pythonosc import udp_client

# A whole-second timestamp in the future: integer seconds round-trip exactly
# through the NTP timetag and OscPacket does not rewrite future timetags.
TIMESTAMP = int(time.time()) + 3600
SMALL_MTU = 108


def _make_bundle(num_messages: int) -> osc_bundle.OscBundle:
    builder = osc_bundle_builder.OscBundleBuilder(TIMESTAMP)
    for index in range(num_messages):
        msg_builder = osc_message_builder.OscMessageBuilder(address=f"/test/{index}")
        msg_builder.add_arg(index)
        builder.add_content(msg_builder.build())
    return builder.build()


class TestUdpClient(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_send(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)

        msg = osc_message_builder.OscMessageBuilder("/").build()
        client.send(msg)

        self.assertTrue(mock_socket.sendto.called)
        mock_socket.sendto.assert_called_once_with(msg.dgram, ("::1", 31337))


class TestUdpClientBundledSend(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_send_bundles_sends_each_datagram_in_order(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        fragments = bundle_splitter.split_bundle(_make_bundle(6), SMALL_MTU)

        sent = client.send_bundles(fragments)

        self.assertEqual(len(fragments), sent)
        self.assertEqual(len(fragments), mock_socket.sendto.call_count)
        expected_calls = [
            mock.call(fragment.dgram, ("::1", 31337)) for fragment in fragments
        ]
        self.assertEqual(expected_calls, mock_socket.sendto.call_args_list)

    @mock.patch("socket.socket")
    def test_send_bundles_empty_sends_nothing(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        self.assertEqual(0, client.send_bundles([]))
        mock_socket.sendto.assert_not_called()

    @mock.patch("socket.socket")
    def test_send_bundles_reports_sent_count_on_interruption(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        fragments = bundle_splitter.split_bundle(_make_bundle(6), SMALL_MTU)
        mock_socket.sendto.side_effect = [None, OSError("network unreachable")]

        with self.assertRaises(udp_client.BundleSendError) as context:
            client.send_bundles(fragments)
        self.assertEqual(1, context.exception.sent_count)
        self.assertEqual(2, mock_socket.sendto.call_count)

    @mock.patch("socket.socket")
    def test_send_fragmented_bundle_sends_all_fragments(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        bundle = _make_bundle(6)
        expected = bundle_splitter.split_bundle(bundle, SMALL_MTU)

        sent = client.send_fragmented_bundle(bundle, SMALL_MTU)

        self.assertEqual(len(expected), sent)
        self.assertEqual(len(expected), mock_socket.sendto.call_count)

    @mock.patch("socket.socket")
    def test_send_fragmented_bundle_build_error_sends_nothing(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        bundle = _make_bundle(1)
        with self.assertRaises(bundle_splitter.BundleBuildError) as context:
            client.send_fragmented_bundle(bundle, bundle_splitter.BUNDLE_HEADER_SIZE)
        # The packing error carries an explicit zero sent count.
        self.assertEqual(0, context.exception.sent_count)
        mock_socket.sendto.assert_not_called()

    @mock.patch("socket.socket")
    def test_send_fragmented_bundle_interruption_count(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        bundle = _make_bundle(100)
        mock_socket.sendto.side_effect = [None, None, OSError("boom")]

        with self.assertRaises(udp_client.BundleSendError) as context:
            client.send_fragmented_bundle(bundle, SMALL_MTU)
        self.assertEqual(2, context.exception.sent_count)


class TestUdpClientBundledSendLoopback(unittest.TestCase):
    """End-to-end check over a real loopback UDP socket."""

    def setUp(self):
        self.receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver.bind(("127.0.0.1", 0))
        self.receiver.settimeout(2.0)
        self._port = self.receiver.getsockname()[1]

    def tearDown(self):
        self.receiver.close()

    def test_100_messages_small_mtu_round_trip_in_order(self):
        num_messages = 100
        bundle = _make_bundle(num_messages)
        client = udp_client.UDPClient("127.0.0.1", self._port)
        try:
            num_datagrams = client.send_fragmented_bundle(bundle, SMALL_MTU)
        finally:
            client.close()

        received_addresses = []
        datagram_sizes = []
        for _ in range(num_datagrams):
            data, _ = self.receiver.recvfrom(4096)
            datagram_sizes.append(len(data))
            packet = osc_packet.OscPacket(data)
            # Every message in every packet carries the shared timetag.
            for timed_message in packet.messages:
                self.assertEqual(TIMESTAMP, timed_message.time)
                received_addresses.append(timed_message.message.address)

        # Every datagram respected the MTU...
        self.assertTrue(all(size <= SMALL_MTU for size in datagram_sizes))
        # ...and more than one datagram was needed for 100 messages.
        self.assertGreater(num_datagrams, 1)
        # Ordering and completeness across the whole transmission.
        self.assertEqual(
            [f"/test/{index}" for index in range(num_messages)],
            received_addresses,
        )


class TestSimpleUdpClient(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch("pythonosc.udp_client.OscMessageBuilder")
        self.patcher.start()
        self.builder = udp_client.OscMessageBuilder.return_value
        self.msg = self.builder.build.return_value
        self.client = mock.Mock()

    def tearDown(self):
        self.patcher.stop()

    def test_send_message_calls_send_with_msg(self):
        udp_client.SimpleUDPClient.send_message(self.client, "/address", 1)
        self.client.send.assert_called_once_with(self.msg)

    def test_send_message_calls_add_arg_with_value(self):
        udp_client.SimpleUDPClient.send_message(self.client, "/address", 1)
        self.builder.add_arg.assert_called_once_with(1)

    def test_send_message_calls_add_arg_once_with_string(self):
        udp_client.SimpleUDPClient.send_message(self.client, "/address", "hello")
        self.builder.add_arg.assert_called_once_with("hello")

    def test_send_message_calls_add_arg_multiple_times_with_list(self):
        udp_client.SimpleUDPClient.send_message(
            self.client, "/address", [1, "john", True]
        )
        self.assertEqual(self.builder.add_arg.call_count, 3)


class TestUdpClientClose(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_close(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337)
        client.close()
        self.assertTrue(mock_socket.close.called)

    @mock.patch("socket.socket")
    def test_context_manager(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        with udp_client.UDPClient("::1", 31337) as client:
            self.assertIsInstance(client, udp_client.UDPClient)
        self.assertTrue(mock_socket.close.called)


class TestUdpClientTimeout(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_init_timeout(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337, timeout=10.0)
        self.assertEqual(client._timeout, 10.0)
        mock_socket.settimeout.assert_any_call(10.0)

    @mock.patch("socket.socket")
    def test_receive_default_timeout(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337, timeout=10.0)
        mock_socket.recv.return_value = b"data"
        client.receive()
        mock_socket.settimeout.assert_called_with(10.0)

    @mock.patch("socket.socket")
    def test_receive_override_timeout(self, mock_socket_ctor):
        mock_socket = mock_socket_ctor.return_value
        client = udp_client.UDPClient("::1", 31337, timeout=10.0)
        mock_socket.recv.return_value = b"data"
        client.receive(timeout=5.0)
        mock_socket.settimeout.assert_called_with(5.0)


if __name__ == "__main__":
    unittest.main()
