"""Maps OSC addresses to handler functions"""

import asyncio
import collections
import inspect
import logging
import re
import threading
import time
from pythonosc import osc_packet
from pythonosc import schema as osc_schema
from typing import (
    overload,
    Dict,
    List,
    Union,
    Any,
    AnyStr,
    Generator,
    Tuple,
    Callable,
    Optional,
    DefaultDict,
)
from pythonosc.osc_message import OscMessage
from pythonosc.osc_message_builder import ArgValue


def _compile_address_pattern(address_pattern: str) -> Optional[re.Pattern]:
    """Compiles an OSC address pattern to a regular expression.

    Spec: https://opensoundcontrol.stanford.edu/spec-1_0.html#osc-address-patterns

    Returns None when the pattern is invalid (it then matches nothing).
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


class Dispatcher(object):
    """Maps Handlers to OSC addresses and dispatches messages to the handler on matched addresses

    Maps OSC addresses to handler functions and invokes the correct handler when a message comes in.
    """

    def __init__(self, strict_timing: bool = True) -> None:
        """Initialize the dispatcher.

        Args:
            strict_timing: Whether to automatically schedule messages with future timetags.
                If True (default), the dispatcher will wait (using sleep) until the specified
                timetag before invoking handlers.
                If False, messages are dispatched immediately regardless of their timetag.
                Disabling this can prevent memory/thread accumulation issues when receiving
                many future-dated messages.
        """
        self._map: DefaultDict[str, List[Handler]] = collections.defaultdict(list)
        self._default_handler: Optional[Handler] = None
        self._strict_timing = strict_timing
        # Guards _map, _schemas and _schema_violations so handlers and
        # schemas can be registered while packets are being dispatched.
        self._lock = threading.RLock()
        self._schemas: Dict[str, osc_schema.MessageSchema] = {}
        self._schema_violations: List[osc_schema.SchemaViolation] = []
        self._schema_error_handler: Optional[
            Callable[[osc_schema.SchemaViolation], None]
        ] = None

    def map(
        self,
        address: str,
        handler: Callable,
        *args: Union[Any, List[Any]],
        needs_reply_address: bool = False,
        schema: Union[None, osc_schema.MessageSchema, str] = None,
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
            schema: Optional MessageSchema (or a type tag string such as "ifs")
                declaring the expected argument count, OSC types and numeric
                ranges for this address. Messages that fail validation are not
                delivered to the handler; a SchemaViolation diagnostic is
                recorded instead. Registering a different schema for an
                address that already has one raises ValueError.

        Returns:
            The handler object that will be invoked should the given address match

        Raises:
            ValueError: If a different schema was already registered for this address.
        """
        # TODO: Check the spec:
        # http://opensoundcontrol.org/spec-1_0
        # regarding multiple mappings
        if schema is not None and not isinstance(schema, osc_schema.MessageSchema):
            schema = osc_schema.MessageSchema(schema)
        handlerobj = Handler(handler, list(args), needs_reply_address)
        with self._lock:
            if schema is not None:
                existing = self._schemas.get(address)
                if existing is not None and existing != schema:
                    raise ValueError(
                        f"Conflicting schema for address '{address}': "
                        f"{existing!r} already registered, got {schema!r}"
                    )
                self._schemas[address] = schema
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
        with self._lock:
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
            if not self._map[address]:
                # No handler left on this address: drop its schema, if any.
                self._schemas.pop(address, None)

    def _schema_for_address(self, address: str) -> Optional[osc_schema.MessageSchema]:
        """Returns the schema whose registered pattern matches an address, if any."""
        with self._lock:
            candidates = list(self._schemas.items())
        for registered, schema in candidates:
            pattern = _compile_address_pattern(registered)
            if pattern is not None and pattern.match(address):
                return schema
            if "*" in registered and re.match(
                registered.replace("*", ".*?") + "$", address
            ):
                return schema
        return None

    @property
    def schema_violations(self) -> List[osc_schema.SchemaViolation]:
        """Returns a copy of the diagnostics recorded so far."""
        with self._lock:
            return list(self._schema_violations)

    def clear_schema_violations(self) -> None:
        """Discards all recorded schema diagnostics."""
        with self._lock:
            self._schema_violations.clear()

    def set_schema_error_handler(
        self, handler: Optional[Callable[[osc_schema.SchemaViolation], None]]
    ) -> None:
        """Sets a callback invoked with every new SchemaViolation diagnostic.

        Args:
            handler: Callback receiving each violation as it is recorded, or
                None to disable notification.
        """
        with self._lock:
            self._schema_error_handler = handler

    def _report_violation(self, violation: osc_schema.SchemaViolation) -> None:
        with self._lock:
            self._schema_violations.append(violation)
            handler = self._schema_error_handler
        logging.warning("OSC schema violation: %s", violation)
        if handler is not None:
            handler(violation)

    def handlers_for_address(
        self, address_pattern: str
    ) -> Generator[Handler, None, None]:
        """Yields handlers matching an address

        Args:
            address_pattern: Address to match

        Returns:
            Generator yielding Handlers matching address_pattern
        """
        # Convert OSC Address Pattern to a Python regular expression.
        # Spec: https://opensoundcontrol.stanford.edu/spec-1_0.html#osc-address-patterns
        patterncompiled = _compile_address_pattern(address_pattern)

        with self._lock:
            map_snapshot = list(self._map.items())
            default_handler = self._default_handler

        matched = False
        if patterncompiled is not None:
            for addr, handlers in map_snapshot:
                if patterncompiled.match(addr) or (
                    "*" in addr
                    and re.match(addr.replace("*", ".*?") + "$", address_pattern)
                ):
                    yield from handlers
                    matched = True

        if not matched and default_handler:
            logging.debug("No handler matched but default handler present, added it.")
            yield default_handler

    def call_handlers_for_packet(
        self, data: bytes, client_address: Tuple[str, int]
    ) -> List:
        """Invoke handlers for all messages in OSC packet

        The incoming OSC Packet is decoded and the handlers for each included message is found and invoked.

        Args:
            data: Data of packet
            client_address: Address of client this packet originated from
        Returns: A list of strings or tuples to be converted to OSC messages and returned to the client
        """
        results = list()
        # Get OSC messages from all bundles or standalone message.
        try:
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                now = time.time()
                handlers = list(self.handlers_for_address(timed_msg.message.address))
                if not handlers:
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > now:
                    time.sleep(timed_msg.time - now)
                schema = self._schema_for_address(timed_msg.message.address)
                if schema is not None:
                    violations = schema.validate(timed_msg.message)
                    if violations:
                        # Invalid message: report and skip the device callback.
                        for violation in violations:
                            self._report_violation(violation)
                        continue
                for handler in handlers:
                    if schema is not None:
                        try:
                            result = handler.invoke(client_address, timed_msg.message)
                        except Exception as exc:
                            self._report_violation(
                                osc_schema.SchemaViolation(
                                    address=timed_msg.message.address,
                                    kind=osc_schema.CALLBACK_ERROR,
                                    exception=exc,
                                )
                            )
                            continue
                    else:
                        result = handler.invoke(client_address, timed_msg.message)
                    if result is not None:
                        results.append(result)
        except osc_packet.ParseError:
            pass
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
        """

        # Get OSC messages from all bundles or standalone message.
        results = []
        try:
            packet = osc_packet.OscPacket(data)
            for timed_msg in packet.messages:
                now = time.time()
                handlers = list(self.handlers_for_address(timed_msg.message.address))
                if not handlers:
                    continue
                # If the message is to be handled later, then so be it.
                if self._strict_timing and timed_msg.time > now:
                    await asyncio.sleep(timed_msg.time - now)
                schema = self._schema_for_address(timed_msg.message.address)
                if schema is not None:
                    violations = schema.validate(timed_msg.message)
                    if violations:
                        # Invalid message: report and skip the device callback.
                        for violation in violations:
                            self._report_violation(violation)
                        continue
                for handler in handlers:
                    if schema is not None:
                        try:
                            result = await handler.async_invoke(
                                client_address, timed_msg.message
                            )
                        except Exception as exc:
                            self._report_violation(
                                osc_schema.SchemaViolation(
                                    address=timed_msg.message.address,
                                    kind=osc_schema.CALLBACK_ERROR,
                                    exception=exc,
                                )
                            )
                            continue
                    else:
                        result = await handler.async_invoke(
                            client_address, timed_msg.message
                        )
                    if result is not None:
                        results.append(result)
        except osc_packet.ParseError:
            pass
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
