"""UDP Clients for sending OSC messages to an OSC server"""

import sys

if sys.version_info > (3, 5):
    from collections.abc import Iterable
else:
    from collections import Iterable

import socket
from typing import Generator, Iterable as TIterable, Union

from pythonosc import bundle_splitter
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_bundle import OscBundle
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue, OscMessageBuilder


class BundleSendError(OSError):
    """Raised when sending a sequence of bounded bundles is interrupted.

    The bundles produced before the failing socket operation have already been
    handed to the OS, so callers need to know how far the send progressed.
    """

    def __init__(self, message: str, sent_count: int) -> None:
        super().__init__(message)
        self._sent_count = sent_count

    @property
    def sent_count(self) -> int:
        """Number of datagrams successfully sent before the failure."""
        return self._sent_count


class UDPClient(object):
    """OSC client to send :class:`OscMessage` or :class:`OscBundle` via UDP"""

    def __init__(
        self,
        address: str,
        port: int,
        allow_broadcast: bool = False,
        family: socket.AddressFamily = socket.AF_UNSPEC,
        timeout: float | None = None,
    ) -> None:
        """Initialize client

        As this is UDP it will not actually make any attempt to connect to the
        given server at ip:port until the send() method is called.

        Args:
            address: IP address of server
            port: Port of server
            allow_broadcast: Allow for broadcast transmissions
            family: address family parameter (passed to socket.getaddrinfo)
            timeout: Default timeout in seconds for socket operations
        """

        for addr in socket.getaddrinfo(
            address, port, type=socket.SOCK_DGRAM, family=family
        ):
            af, socktype, protocol, canonname, sa = addr

            try:
                self._sock = socket.socket(af, socktype)
            except OSError:
                continue
            break

        self._sock.setblocking(False)
        if timeout is not None:
            self._sock.settimeout(timeout)
        self._timeout = timeout

        if allow_broadcast:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._address = address
        self._port = port

    def __enter__(self) -> "UDPClient":
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the socket"""
        self._sock.close()

    def send(self, content: Union[OscMessage, OscBundle]) -> None:
        """Sends an :class:`OscMessage` or :class:`OscBundle` via UDP

        Args:
            content: Message or bundle to be sent
        """
        self._sock.sendto(content.dgram, (self._address, self._port))

    def send_bundles(self, bundles: TIterable[OscBundle]) -> int:
        """Send several bundles in order, one datagram per bundle.

        Args:
            bundles: bundles to send, in transmission order.

        Returns:
            the number of datagrams actually sent.

        Raises:
            BundleSendError: if a socket operation fails midway. The number
                of datagrams already sent is available on the exception's
                ``sent_count`` attribute.
        """
        sent_count = 0
        for bundle in bundles:
            try:
                self.send(bundle)
            except OSError as ose:
                raise BundleSendError(
                    f"Sending bundle {sent_count} failed: {ose}", sent_count
                ) from ose
            sent_count += 1
        return sent_count

    def send_fragmented_bundle(self, bundle: OscBundle, max_datagram_size: int) -> int:
        """Split one bundle into bounded datagrams and send them in order.

        The elements sharing ``bundle``'s timetag are distributed over several
        legal OSC bundles, each no larger than ``max_datagram_size``, and sent
        sequentially. Individual messages and nested bundles are never split.

        Args:
            bundle: the bundle whose elements should be distributed.
            max_datagram_size: maximum size in bytes of any emitted datagram.

        Returns:
            the number of datagrams actually sent.

        Raises:
            bundle_splitter.BundleBuildError: if the bundle cannot be split
                (empty input, a limit smaller than the bundle header, a
                timetag mismatch, or an element that does not fit alone).
                Nothing has been sent when this is raised.
            BundleSendError: if a socket operation fails midway. The number
                of datagrams already sent is available on the exception's
                ``sent_count`` attribute.
        """
        # Build everything first: a packing failure must not leave
        # partially transmitted state.
        fragments = bundle_splitter.split_bundle(bundle, max_datagram_size)
        return self.send_bundles(fragments)

    def receive(self, timeout: float | None = None) -> bytes:
        """Wait :int:`timeout` seconds for a message an return the raw bytes

        Args:
            timeout: Number of seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        if timeout is not None:
            self._sock.settimeout(timeout)
        elif self._timeout is not None:
            self._sock.settimeout(self._timeout)

        try:
            return self._sock.recv(4096)
        except (TimeoutError, socket.timeout, BlockingIOError):
            return b""


class SimpleUDPClient(UDPClient):
    """Simple OSC client that automatically builds :class:`OscMessage` from arguments"""

    def send_message(
        self, address: str, value: Union[ArgValue, Iterable[ArgValue]]
    ) -> None:
        """Build :class:`OscMessage` from arguments and send to server

        Args:
            address: OSC address the message shall go to
            value: One or more arguments to be added to the message
        """
        builder = OscMessageBuilder(address=address)
        if value is None:
            pass
        elif not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
            builder.add_arg(value)
        else:
            for val in value:
                builder.add_arg(val)
        msg = builder.build()
        self.send(msg)

    def get_messages(self, timeout: float | None = None) -> Generator:
        """Wait :int:`timeout` seconds for a message from the server and convert it to a :class:`OscMessage`

        Args:
            timeout: Time in seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        msg = self.receive(timeout)
        while msg:
            yield OscMessage(msg)
            msg = self.receive(timeout)


class DispatchClient(SimpleUDPClient):
    """OSC Client that includes a :class:`Dispatcher` for handling responses and other messages from the server"""

    dispatcher = Dispatcher()

    def handle_messages(self, timeout: float | None = None) -> None:
        """Wait :int:`timeout` seconds for a message from the server and process each message with the registered
        handlers.  Continue until a timeout occurs.

        Args:
            timeout: Time in seconds to wait for a message.
                     If None, uses the default timeout set in __init__.
        """
        msg = self.receive(timeout)
        while msg:
            self.dispatcher.call_handlers_for_packet(msg, (self._address, self._port))
            msg = self.receive(timeout)
