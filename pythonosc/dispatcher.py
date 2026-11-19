"""Maps OSC addresses to handler functions"""

import asyncio
import collections
import inspect
import logging
import re
import threading
import time
from pythonosc import osc_packet
from typing import (
    overload,
    List,
    Union,
    Any,
    AnyStr,
    Dict,
    Generator,
    Tuple,
    Callable,
    Optional,
    DefaultDict,
    NamedTuple,
)
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue

logger = logging.getLogger(__name__)

# Interval between cancellation checks while waiting for a future timetag.
_CANCEL_CHECK_INTERVAL = 0.1


class BatchMessage(NamedTuple):
    """A single OSC message as delivered to a bundle (batch) handler.

    Preserves the message's original address and OSC arguments. Batches are
    delivered in the order the messages appeared inside their bundle.
    """

    address: str
    args: Tuple[Any, ...]


class Handler(object):
    """Wrapper for a callback function that will be called when an OSC message is sent to the right address.

    Represents a handler callback function that will be called whenever an OSC message is sent to the address this
    handler is mapped to. It passes the address, the fixed arguments (if any) as well as all osc arguments from the
    message if any were passed.
    """

    def __init__(
        self,
        _callback: Callable,
        _args: Union[Any, List[Any]],
        _needs_reply_address: bool = False,
    ) -> None:
        """
        Args:
            _callback Function that is called when handler is invoked
            _args: Message causing invocation
            _needs_reply_address Whether the client's ip address shall be passed as an argument or not
        """
        self.callback = _callback
        self.args = _args
        self.needs_reply_address = _needs_reply_address

    # needed for test module
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(self, type(other))
            and self.callback == other.callback
            and self.args == other.args
            and self.needs_reply_address == other.needs_reply_address
        )

    def invoke(
        self, client_address: Tuple[str, int], message: OscMessage
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function

        Args:
            client_address: Address match that causes the invocation
            message: Message causing invocation
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        if self.needs_reply_address:
            if self.args:
                return self.callback(
                    client_address, message.address, self.args, *message
                )
            else:
                return self.callback(client_address, message.address, *message)
        else:
            if self.args:
                return self.callback(message.address, self.args, *message)
            else:
                return self.callback(message.address, *message)

    async def async_invoke(
        self, client_address: Tuple[str, int], message: OscMessage
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function (asynchronously)

        Args:
            client_address: Address match that causes the invocation
            message: Message causing invocation
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        cb = self.callback
        is_async = inspect.iscoroutinefunction(cb)

        if self.needs_reply_address:
            if self.args:
                if is_async:
                    return await cb(
                        client_address, message.address, self.args, *message
                    )
                else:
                    return cb(client_address, message.address, self.args, *message)
            else:
                if is_async:
                    return await cb(client_address, message.address, *message)
                else:
                    return cb(client_address, message.address, *message)
        else:
            if self.args:
                if is_async:
                    return await cb(message.address, self.args, *message)
                else:
                    return cb(message.address, self.args, *message)
            else:
                if is_async:
                    return await cb(message.address, *message)
                else:
                    return cb(message.address, *message)


class BundleHandler(object):
    """Wrapper for a callback invoked once per bundle batch instead of once per message.

    The callback receives the whole batch of matched messages at once, as a list of
    BatchMessage, so stateful receivers (e.g. a lighting desk applying color
    temperature, brightness and hue together) never observe intermediate states.

    The callback function must have one of the following signatures:

    .. code-block:: python

        def some_cb(messages: List[BatchMessage]) -> Union[None, str, Tuple[str, ArgValue]]: ...
        def some_cb(messages: List[BatchMessage], fixed_args: List[Any]) -> Union[None, str, Tuple[str, ArgValue]]: ...
        def some_cb(client_address: Tuple[str, int], messages: List[BatchMessage]) -> Union[None, str, Tuple[str, ArgValue]]: ...
        def some_cb(client_address: Tuple[str, int], messages: List[BatchMessage], fixed_args: List[Any]) -> Union[None, str, Tuple[str, ArgValue]]: ...
    """

    def __init__(
        self,
        _callback: Callable,
        _args: Union[Any, List[Any]],
        _needs_reply_address: bool = False,
    ) -> None:
        """
        Args:
            _callback Function that is called when the batch handler is invoked
            _args: Fixed arguments passed to the callback after the batch
            _needs_reply_address Whether the client's ip address shall be passed as an argument or not
        """
        self.callback = _callback
        self.args = _args
        self.needs_reply_address = _needs_reply_address

    # Identity hash: batches are keyed by the registered handler object.
    __hash__ = object.__hash__

    # needed for test module
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(self, type(other))
            and self.callback == other.callback
            and self.args == other.args
            and self.needs_reply_address == other.needs_reply_address
        )

    def _build_call_args(
        self, client_address: Tuple[str, int], batch: List[BatchMessage]
    ) -> List[Any]:
        call_args: List[Any] = []
        if self.needs_reply_address:
            call_args.append(client_address)
        call_args.append(batch)
        if self.args:
            call_args.append(self.args)
        return call_args

    def invoke(
        self, client_address: Tuple[str, int], batch: List[BatchMessage]
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function with the whole batch at once.

        Args:
            client_address: Address match that causes the invocation
            batch: Matched messages, in bundle order, with their original addresses
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        return self.callback(*self._build_call_args(client_address, batch))

    async def async_invoke(
        self, client_address: Tuple[str, int], batch: List[BatchMessage]
    ) -> Union[None, AnyStr, Tuple[AnyStr, ArgValue]]:
        """Invokes the associated callback function (asynchronously)

        Args:
            client_address: Address match that causes the invocation
            batch: Matched messages, in bundle order, with their original addresses
        Returns:
            The result of the handler function can be None, a string OSC address, or a tuple of the OSC address
            and arguments.
        """
        cb = self.callback
        call_args = self._build_call_args(client_address, batch)
        if inspect.iscoroutinefunction(cb):
            return await cb(*call_args)
        else:
            return cb(*call_args)


def _compile_address_pattern(address_pattern: str) -> "Optional[re.Pattern[str]]":
    """Compiles an OSC address pattern into a regular expression.

    Spec: https://opensoundcontrol.stanford.edu/spec-1_0.html#osc-address-patterns

    Returns:
        The compiled pattern, or None if the pattern is invalid (in which case
        it cannot match anything).
    """
    pattern = "^"
    i = 0
    while i < len(address_pattern):
        c = address_pattern[i]
        if c == "*":
            pattern += "[^/]*"
        elif c == "?":
            pattern += "[^/]"
        elif c == "[":
            pattern += "["
            i += 1
            if i < len(address_pattern) and address_pattern[i] == "!":
                pattern += "^"
                i += 1
            while i < len(address_pattern) and address_pattern[i] != "]":
                if address_pattern[i] in r"\^$.|()+*?":
                    pattern += "\\"
                pattern += address_pattern[i]
                i += 1
            pattern += "]"
        elif c == "{":
            pattern += "("
            i += 1
            while i < len(address_pattern) and address_pattern[i] != "}":
                char = address_pattern[i]
                if char == ",":
                    pattern += "|"
                elif char in r"\^$.|()[]+*?":
                    pattern += "\\" + char
                else:
                    pattern += char
                i += 1
            pattern += ")"
        elif c in r"\^$.|()[]+?":
            pattern += "\\" + c
        else:
            pattern += c
        i += 1
    pattern += "$"

    try:
        return re.compile(pattern)
    except re.error:
        # If the pattern is invalid, it won't match anything.
        return None


class Dispatcher(object):
    """Maps Handlers to OSC addresses and dispatches messages to the handler on matched addresses

    Maps OSC addresses to handler functions and invokes the correct handler when a message comes in.
    """

    def __init__(
        self,
        strict_timing: bool = True,
        max_batch_size: int = 1024,
        time_func: Callable[[], float] = time.time,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            strict_timing: Whether to automatically schedule messages with future timetags.
                If True (default), the dispatcher will wait (using sleep) until the specified
                timetag before invoking handlers.
                If False, messages are dispatched immediately regardless of their timetag.
                Disabling this can prevent memory/thread accumulation issues when receiving
                many future-dated messages.
            max_batch_size: Maximum number of messages delivered to a bundle handler in a
                single call. Larger batches are split into several calls of at most this
                many messages, preserving order. Must be at least 1.
            time_func: Function returning the current system time in seconds since the
                epoch. Injectable for tests (virtual clock).
            sleep_func: Function sleeping the given number of seconds in the synchronous
                dispatch path. Injectable for tests (virtual clock).

        Raises:
            ValueError: if max_batch_size is smaller than 1.
        """
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be at least 1")
        self._map: DefaultDict[str, List[Handler]] = collections.defaultdict(list)
        self._bundle_map: DefaultDict[str, List[BundleHandler]] = (
            collections.defaultdict(list)
        )
        self._default_handler: Optional[Handler] = None
        self._strict_timing = strict_timing
        self._max_batch_size = max_batch_size
        self._time_func = time_func
        self._sleep_func = sleep_func
        self._cancel_event = threading.Event()

    def map(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> Handler:
        """Map an address to a handler

        The callback function must have one of the following signatures:

        .. code-block:: python

            def some_cb(address: str, *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(address: str, fixed_args: List[Any], *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], address: str, *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], address: str, fixed_args: List[Any], *osc_args: ArgValue) -> Union[None, str, Tuple[str, ArgValue]]: ...

        Each variadic OSC argument is a single ArgValue, not a list of all arguments.
        Any fixed arguments passed to map() are collected into the separate fixed_args list.

        The callback function can return None, or a string representing an OSC address to be returned to the client,
        or a tuple that includes the address and ArgValue which will be converted to an OSC message and returned to
        the client.

        Args:
            address: Address to be mapped
            handler: Callback function that will be called as the handler for the given address
            *args: Fixed arguements that will be passed to the callback function
            needs_reply_address: Whether the IP address from which the message originated from shall be passed as
                an argument to the handler callback

        Returns:
            The handler object that will be invoked should the given address match

        """
        # TODO: Check the spec:
        # http://opensoundcontrol.org/spec-1_0
        # regarding multiple mappings
        handlerobj = Handler(handler, list(args), needs_reply_address)
        self._map[address].append(handlerobj)
        return handlerobj

    @overload
    def unmap(self, address: str, handler: Handler) -> None:
        """Remove an already mapped handler from an address

        Args:
            address (str): Address to be unmapped
            handler (Handler): A Handler object as returned from map().
        """
        pass

    @overload
    def unmap(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> None:
        """Remove an already mapped handler from an address

        Args:
            address: Address to be unmapped
            handler: A function that will be run when the address matches with
                the OscMessage passed as parameter.
            args: Any additional arguments that will be always passed to the
                handlers after the osc messages arguments if any.
            needs_reply_address: True if the handler function needs the
                originating client address passed (as the first argument).
        """
        pass

    def unmap(self, address, handler, *args, needs_reply_address=False):
        try:
            if isinstance(handler, Handler):
                self._map[address].remove(handler)
            else:
                self._map[address].remove(
                    Handler(handler, list(args), needs_reply_address)
                )
        except ValueError as e:
            if str(e) == "list.remove(x): x not in list":
                raise ValueError(
                    f"Address '{address}' doesn't have handler '{handler}' mapped to it"
                ) from e

    def map_bundle(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> BundleHandler:
        """Map an address to a batch handler that receives bundled messages at once.

        For every incoming packet, messages that share the same effective timetag
        (i.e. that belong to the same bundle, nested bundles keeping their own
        timetags) and match this address are collected and delivered to the
        handler in a single call, as a list of BatchMessage preserving the
        in-bundle order and original addresses.

        The callback function must have one of the following signatures:

        .. code-block:: python

            def some_cb(messages: List[BatchMessage]) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(messages: List[BatchMessage], fixed_args: List[Any]) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], messages: List[BatchMessage]) -> Union[None, str, Tuple[str, ArgValue]]: ...
            def some_cb(client_address: Tuple[str, int], messages: List[BatchMessage], fixed_args: List[Any]) -> Union[None, str, Tuple[str, ArgValue]]: ...

        Args:
            address: Address (or address pattern) to be mapped
            handler: Callback function invoked once per batch of matched messages
            *args: Fixed arguments that will be passed to the callback function
            needs_reply_address: Whether the IP address from which the messages originated
                shall be passed as an argument to the handler callback

        Returns:
            The BundleHandler object that will be invoked should the given address match
        """
        handlerobj = BundleHandler(handler, list(args), needs_reply_address)
        self._bundle_map[address].append(handlerobj)
        return handlerobj

    @overload
    def unmap_bundle(self, address: str, handler: BundleHandler) -> None:
        """Remove an already mapped batch handler from an address

        Args:
            address (str): Address to be unmapped
            handler (BundleHandler): A BundleHandler object as returned from map_bundle().
        """
        pass

    @overload
    def unmap_bundle(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
    ) -> None:
        """Remove an already mapped batch handler from an address

        Args:
            address: Address to be unmapped
            handler: The callback function that was mapped with map_bundle().
            args: Any additional arguments that were registered with the handler.
            needs_reply_address: True if the handler was registered with the
                originating client address passed as the first argument.
        """
        pass

    def unmap_bundle(self, address, handler, *args, needs_reply_address=False):
        try:
            if isinstance(handler, BundleHandler):
                self._bundle_map[address].remove(handler)
            else:
                self._bundle_map[address].remove(
                    BundleHandler(handler, list(args), needs_reply_address)
                )
        except ValueError as e:
            if str(e) == "list.remove(x): x not in list":
                raise ValueError(
                    f"Address '{address}' doesn't have batch handler '{handler}' mapped to it"
                ) from e

    def cancel_pending(self) -> None:
        """Request cancellation of not-yet-delivered future-timetag batches.

        Dispatch methods sleeping until a future timetag abort the wait and skip
        the remaining groups of the packet currently being dispatched; results
        collected so far are returned. The cancellation stays in effect for
        subsequent packets until clear_cancellation() is called.
        """
        self._cancel_event.set()

    def clear_cancellation(self) -> None:
        """Re-arm dispatch after a previous cancel_pending() call."""
        self._cancel_event.clear()

    def handlers_for_address(
        self, address_pattern: str
    ) -> Generator[Handler, None, None]:
        """Yields handlers matching an address

        Args:
            address_pattern: Address to match

        Returns:
            Generator yielding Handlers matching address_pattern
        """
        patterncompiled = _compile_address_pattern(address_pattern)
        if patterncompiled is None:
            # If the pattern is invalid, it won't match anything.
            return

        matched = False
        for addr, handlers in self._map.items():
            if patterncompiled.match(addr) or (
                "*" in addr
                and re.match(addr.replace("*", ".*?") + "$", address_pattern)
            ):
                yield from handlers
                matched = True

        if not matched and self._default_handler:
            logger.debug("No handler matched but default handler present, added it.")
            yield self._default_handler

    def bundle_handlers_for_address(
        self, address_pattern: str
    ) -> Generator[BundleHandler, None, None]:
        """Yields batch handlers matching an address

        Args:
            address_pattern: Address to match

        Returns:
            Generator yielding BundleHandlers matching address_pattern
        """
        patterncompiled = _compile_address_pattern(address_pattern)
        if patterncompiled is None:
            # If the pattern is invalid, it won't match anything.
            return

        for addr, handlers in self._bundle_map.items():
            if patterncompiled.match(addr) or (
                "*" in addr
                and re.match(addr.replace("*", ".*?") + "$", address_pattern)
            ):
                yield from handlers

    @staticmethod
    def _group_messages(
        timed_messages: List[osc_packet.TimedMessage],
    ) -> List[Tuple[float, List[OscMessage]]]:
        """Groups time-sorted TimedMessages by identical effective timetag.

        Messages keep their original in-bundle order within a group, and groups
        are ordered by timetag. Nested bundles with their own timetags end up in
        their own groups.
        """
        groups: List[Tuple[float, List[OscMessage]]] = []
        for timed_msg in timed_messages:
            if groups and groups[-1][0] == timed_msg.time:
                groups[-1][1].append(timed_msg.message)
            else:
                groups.append((timed_msg.time, [timed_msg.message]))
        return groups

    def _wait_until(self, when: float) -> bool:
        """Sleeps until the given system time, honoring cancellation.

        Returns:
            True if the timetag was reached, False if dispatch was cancelled.
        """
        if not self._strict_timing:
            return not self._cancel_event.is_set()
        while True:
            if self._cancel_event.is_set():
                return False
            remaining = when - self._time_func()
            if remaining <= 0:
                return True
            self._sleep_func(min(remaining, _CANCEL_CHECK_INTERVAL))

    async def _async_wait_until(self, when: float) -> bool:
        """Async variant of _wait_until."""
        if not self._strict_timing:
            return not self._cancel_event.is_set()
        while True:
            if self._cancel_event.is_set():
                return False
            remaining = when - self._time_func()
            if remaining <= 0:
                return True
            await asyncio.sleep(min(remaining, _CANCEL_CHECK_INTERVAL))

    def _collect_batches(
        self, messages: List[OscMessage]
    ) -> List[Tuple[BundleHandler, List[BatchMessage]]]:
        """Matches messages against batch handlers, preserving bundle order.

        Returns:
            A list of (handler, batch) pairs, in order of first match. Each
            message may appear in the batches of several handlers.
        """
        matched: Dict[BundleHandler, List[BatchMessage]] = {}
        for message in messages:
            for handler in self.bundle_handlers_for_address(message.address):
                matched.setdefault(handler, []).append(
                    BatchMessage(message.address, tuple(message))
                )
        return list(matched.items())

    def _invoke_bundle_handlers(
        self,
        client_address: Tuple[str, int],
        messages: List[OscMessage],
        results: List,
    ) -> None:
        """Delivers one batch (chunked to max_batch_size) per matched bundle handler.

        A bundle handler raising an exception is logged and skipped; other
        handlers and chunks are unaffected.
        """
        for handler, batch in self._collect_batches(messages):
            for start in range(0, len(batch), self._max_batch_size):
                chunk = batch[start : start + self._max_batch_size]
                try:
                    result = handler.invoke(client_address, chunk)
                except Exception:
                    logger.exception(
                        "Bundle handler %r raised an exception; "
                        "skipping the rest of its batch",
                        handler.callback,
                    )
                    break
                if result is not None:
                    results.append(result)

    async def _async_invoke_bundle_handlers(
        self,
        client_address: Tuple[str, int],
        messages: List[OscMessage],
        results: List,
    ) -> None:
        """Async variant of _invoke_bundle_handlers."""
        for handler, batch in self._collect_batches(messages):
            for start in range(0, len(batch), self._max_batch_size):
                chunk = batch[start : start + self._max_batch_size]
                try:
                    result = await handler.async_invoke(client_address, chunk)
                except Exception:
                    logger.exception(
                        "Bundle handler %r raised an exception; "
                        "skipping the rest of its batch",
                        handler.callback,
                    )
                    break
                if result is not None:
                    results.append(result)

    def call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """Invoke handlers for all messages in OSC packet

        The incoming OSC Packet is decoded and the handlers for each included message is found and invoked.
        Messages sharing an effective timetag (e.g. coming from the same bundle) are dispatched as a group:
        plain handlers are invoked per message in bundle order, then each matching batch handler registered
        with map_bundle() is invoked once with the whole group.

        Args:
            data: Data of packet
            client_address: Address of client this packet originated from
        Returns: A list of strings or tuples to be converted to OSC messages and returned to the client.
            An unparseable packet yields an empty list and no handler invocation.
        """
        results: List = list()
        # Get OSC messages from all bundles or standalone message.
        try:
            packet = osc_packet.OscPacket(data, now=self._time_func())
        except osc_packet.ParseError:
            logger.warning("Dropped unparseable OSC packet", exc_info=True)
            return results
        for when, messages in self._group_messages(packet.messages):
            # If the messages are to be handled later, then so be it.
            if not self._wait_until(when):
                logger.info(
                    "Dispatch cancelled, skipping %d remaining message(s) of packet",
                    len(messages),
                )
                break
            for message in messages:
                for handler in self.handlers_for_address(message.address):
                    result = handler.invoke(client_address, message)
                    if result is not None:
                        results.append(result)
            self._invoke_bundle_handlers(client_address, messages, results)
        return results

    async def async_call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """
        This function calls the handlers registered to the dispatcher for
        every message it found in the packet.
        The process/thread granularity is thus the OSC packet, not the handler.

        If parameters were registered with the dispatcher, then the handlers are
        called this way:
          handler('/address that triggered the message',
                  registered_param_list, osc_msg_arg1, osc_msg_arg2, ...)
        if no parameters were registered, then it is just called like this:
          handler('/address that triggered the message',
                  osc_msg_arg1, osc_msg_arg2, osc_msg_param3, ...)

        Batch handlers registered with map_bundle() are called once per
        same-timetag group with a list of BatchMessage instead.
        """

        # Get OSC messages from all bundles or standalone message.
        results: List = []
        try:
            packet = osc_packet.OscPacket(data, now=self._time_func())
        except osc_packet.ParseError:
            logger.warning("Dropped unparseable OSC packet", exc_info=True)
            return results
        for when, messages in self._group_messages(packet.messages):
            # If the messages are to be handled later, then so be it.
            if not await self._async_wait_until(when):
                logger.info(
                    "Dispatch cancelled, skipping %d remaining message(s) of packet",
                    len(messages),
                )
                break
            for message in messages:
                for handler in self.handlers_for_address(message.address):
                    result = await handler.async_invoke(client_address, message)
                    if result is not None:
                        results.append(result)
            await self._async_invoke_bundle_handlers(client_address, messages, results)
        return results

    def set_default_handler(
        self, handler: Callable, needs_reply_address: bool = False
    ) -> None:
        """Sets the default handler

        The default handler is invoked every time no other handler is mapped to an address.

        Args:
            handler: Callback function to handle unmapped requests
            needs_reply_address: Whether the callback shall be passed the client address
        """
        self._default_handler = (
            None if (handler is None) else Handler(handler, [], needs_reply_address)
        )
