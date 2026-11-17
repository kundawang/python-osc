import threading
import unittest

from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_bundle_builder import OscBundleBuilder, IMMEDIATELY
from pythonosc.osc_message_builder import OscMessageBuilder
from pythonosc.schema import (
    CALLBACK_ERROR,
    INVALID_TYPE,
    MISSING_ARGUMENT,
    NON_FINITE_VALUE,
    OUT_OF_RANGE,
    UNEXPECTED_ARGUMENT,
    MessageSchema,
    Param,
    SchemaViolation,
)

CLIENT = ("127.0.0.1", 1234)


def build_message(address, *typed_args):
    """Builds an OscMessage, each arg being a value or a (type, value) tuple."""
    builder = OscMessageBuilder(address=address)
    for arg in typed_args:
        if isinstance(arg, tuple):
            builder.add_arg(arg[1], arg[0])
        else:
            builder.add_arg(arg)
    return builder.build()


def build_bundle(*contents):
    builder = OscBundleBuilder(IMMEDIATELY)
    for content in contents:
        builder.add_content(content)
    return builder.build()


class TestMessageSchema(unittest.TestCase):
    def test_schema_from_string_shorthand(self):
        schema = MessageSchema("ifs")
        self.assertEqual(
            [Param("i"), Param("f"), Param("s")], list(schema.params)
        )

    def test_param_rejects_range_on_non_numeric(self):
        with self.assertRaises(ValueError):
            Param("s", minimum=0, maximum=10)

    def test_param_rejects_inverted_range(self):
        with self.assertRaises(ValueError):
            Param("i", minimum=10, maximum=0)

    def test_param_rejects_unknown_type(self):
        with self.assertRaises(ValueError):
            Param("Z")


class TestSchemaRouting(unittest.TestCase):
    def setUp(self):
        self.dispatcher = Dispatcher()
        self.received = []

    def map_light(self, address="/light/1", schema=None):
        schema = schema if schema is not None else MessageSchema(
            [Param("i", minimum=0, maximum=127), Param("f")]
        )
        self.dispatcher.map(
            address, lambda addr, *args: self.received.append((addr, args)),
            schema=schema,
        )

    def dispatch(self, *messages_or_bundles):
        for content in messages_or_bundles:
            self.dispatcher.call_handlers_for_packet(content.dgram, CLIENT)

    def test_valid_message_reaches_handler(self):
        self.map_light()
        self.dispatch(build_message("/light/1", 3, 0.5))
        self.assertEqual([("/light/1", (3, 0.5))], self.received)
        self.assertEqual([], self.dispatcher.schema_violations)

    def test_missing_argument_is_rejected(self):
        self.map_light()
        self.dispatch(build_message("/light/1", 3))
        self.assertEqual([], self.received)
        (violation,) = self.dispatcher.schema_violations
        self.assertEqual("/light/1", violation.address)
        self.assertEqual(MISSING_ARGUMENT, violation.kind)
        self.assertEqual(1, violation.index)

    def test_unexpected_argument_is_rejected(self):
        self.map_light()
        self.dispatch(build_message("/light/1", 3, 0.5, "extra"))
        self.assertEqual([], self.received)
        (violation,) = self.dispatcher.schema_violations
        self.assertEqual(UNEXPECTED_ARGUMENT, violation.kind)
        self.assertEqual(2, violation.index)

    def test_wrong_type_is_rejected(self):
        self.map_light()
        self.dispatch(build_message("/light/1", 0.5, 0.5))
        self.assertEqual([], self.received)
        (violation,) = self.dispatcher.schema_violations
        self.assertEqual(INVALID_TYPE, violation.kind)
        self.assertEqual(0, violation.index)
        self.assertIn("i", violation.expected)
        self.assertIn("f", violation.actual)

    def test_non_finite_values_are_rejected(self):
        self.map_light()
        self.dispatch(
            build_message("/light/1", 3, float("nan")),
            build_message("/light/1", 3, float("inf")),
            build_message("/light/1", 3, float("-inf")),
        )
        self.assertEqual([], self.received)
        violations = self.dispatcher.schema_violations
        self.assertEqual(3, len(violations))
        for violation in violations:
            self.assertEqual(NON_FINITE_VALUE, violation.kind)
            self.assertEqual(1, violation.index)

    def test_out_of_range_is_rejected(self):
        self.map_light()
        self.dispatch(
            build_message("/light/1", 128, 0.5),
            build_message("/light/1", -1, 0.5),
        )
        self.assertEqual([], self.received)
        violations = self.dispatcher.schema_violations
        self.assertEqual(2, len(violations))
        self.assertEqual([OUT_OF_RANGE, OUT_OF_RANGE], [v.kind for v in violations])
        self.assertEqual([0, 0], [v.index for v in violations])

    def test_boundary_values_are_accepted(self):
        self.map_light()
        self.dispatch(
            build_message("/light/1", 0, 0.0),
            build_message("/light/1", 127, 1.0),
        )
        self.assertEqual(2, len(self.received))
        self.assertEqual([], self.dispatcher.schema_violations)

    def test_multiple_acceptable_types(self):
        self.map_light(schema=MessageSchema([Param("if", minimum=0, maximum=1)]))
        self.dispatch(build_message("/light/1", 1), build_message("/light/1", 0.5))
        self.assertEqual(2, len(self.received))

    def test_conflicting_schema_registration_raises(self):
        self.map_light()
        with self.assertRaises(ValueError):
            self.dispatcher.map(
                "/light/1", lambda addr, *args: None, schema=MessageSchema("s")
            )

    def test_identical_schema_reregistration_is_allowed(self):
        self.map_light()
        self.map_light()  # same schema, no conflict
        self.dispatch(build_message("/light/1", 3, 0.5))
        # Both handlers are invoked for the valid message.
        self.assertEqual(2, len(self.received))

    def test_callback_exception_is_reported_and_dispatch_continues(self):
        def boom(address, *args):
            raise RuntimeError("device blew up")

        self.dispatcher.map("/light/1", boom, schema=MessageSchema("i"))
        self.dispatcher.map(
            "/light/2",
            lambda addr, *args: self.received.append(args),
            schema=MessageSchema("i"),
        )
        self.dispatch(
            build_bundle(
                build_message("/light/1", 1),
                build_message("/light/2", 2),
            )
        )
        self.assertEqual([(2,)], self.received)
        (violation,) = self.dispatcher.schema_violations
        self.assertEqual(CALLBACK_ERROR, violation.kind)
        self.assertEqual("/light/1", violation.address)
        self.assertIsInstance(violation.exception, RuntimeError)

    def test_nested_bundle_messages_validated_independently(self):
        self.map_light("/light/1")
        self.map_light("/light/2")
        inner = build_bundle(
            build_message("/light/1", 10, 0.25),  # valid
            build_message("/light/2", 999, 0.5),  # out of range
            build_message("/light/1", 1),  # missing argument
        )
        outer = build_bundle(
            build_message("/light/2", 7, 0.75),  # valid
            inner,
            build_message("/light/1", 5, 0.5, 6),  # unexpected argument
        )
        self.dispatch(outer)
        self.assertEqual(
            [("/light/2", (7, 0.75)), ("/light/1", (10, 0.25))],
            self.received,
        )
        violations = self.dispatcher.schema_violations
        self.assertEqual(3, len(violations))
        by_kind = {v.kind: v for v in violations}
        self.assertEqual("/light/2", by_kind[OUT_OF_RANGE].address)
        self.assertEqual(0, by_kind[OUT_OF_RANGE].index)
        self.assertEqual("/light/1", by_kind[MISSING_ARGUMENT].address)
        self.assertEqual(1, by_kind[MISSING_ARGUMENT].index)
        self.assertEqual("/light/1", by_kind[UNEXPECTED_ARGUMENT].address)
        self.assertEqual(2, by_kind[UNEXPECTED_ARGUMENT].index)

    def test_schema_error_handler_is_notified(self):
        notified = []
        self.dispatcher.set_schema_error_handler(notified.append)
        self.map_light()
        self.dispatch(build_message("/light/1", 3))
        self.assertEqual(1, len(notified))
        self.assertIsInstance(notified[0], SchemaViolation)
        self.assertEqual(MISSING_ARGUMENT, notified[0].kind)

    def test_clear_schema_violations(self):
        self.map_light()
        self.dispatch(build_message("/light/1", 3))
        self.assertEqual(1, len(self.dispatcher.schema_violations))
        self.dispatcher.clear_schema_violations()
        self.assertEqual([], self.dispatcher.schema_violations)

    def test_route_without_schema_keeps_previous_behavior(self):
        self.dispatcher.map(
            "/anything", lambda addr, *args: self.received.append(args)
        )
        # Any argument count/type goes through, as before.
        self.dispatch(
            build_message("/anything"),
            build_message("/anything", 1, "two", 3.0),
        )
        self.assertEqual([(), (1, "two", 3.0)], self.received)
        self.assertEqual([], self.dispatcher.schema_violations)

    def test_callback_exception_without_schema_still_propagates(self):
        def boom(address, *args):
            raise RuntimeError("unhandled")

        self.dispatcher.map("/plain", boom)
        with self.assertRaises(RuntimeError):
            self.dispatch(build_message("/plain", 1))

    def test_unmap_drops_schema(self):
        handler = lambda addr, *args: self.received.append(args)  # noqa: E731
        self.dispatcher.map("/light/1", handler, schema=MessageSchema("i"))
        self.dispatcher.unmap("/light/1", handler)
        self.assertIsNone(self.dispatcher._schema_for_address("/light/1"))
        # A different schema may now be registered without conflict.
        self.dispatcher.map("/light/1", handler, schema=MessageSchema("s"))

    def test_wildcard_pattern_schema(self):
        self.dispatcher.map(
            "/light/*",
            lambda addr, *args: self.received.append((addr, args)),
            schema=MessageSchema("i"),
        )
        self.dispatch(
            build_message("/light/1", 1),
            build_message("/light/2", "nope"),
        )
        self.assertEqual([("/light/1", (1,))], self.received)
        (violation,) = self.dispatcher.schema_violations
        self.assertEqual("/light/2", violation.address)
        self.assertEqual(INVALID_TYPE, violation.kind)

    def test_concurrent_registration_and_dispatch(self):
        dispatcher = Dispatcher()
        received = []
        received_lock = threading.Lock()
        errors = []

        def worker(index):
            address = f"/light/{index}"

            def cb(addr, *args):
                with received_lock:
                    received.append(addr)

            try:
                for _ in range(20):
                    dispatcher.map(address, cb, schema=MessageSchema("i"))
                    valid = build_message(address, index)
                    invalid = build_message(address, "bad")
                    dispatcher.call_handlers_for_packet(valid.dgram, CLIENT)
                    dispatcher.call_handlers_for_packet(invalid.dgram, CLIENT)
                    dispatcher.unmap(address, cb)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual([], errors)
        # Each of the 8 threads dispatched 20 valid messages to 1 handler.
        self.assertEqual(8 * 20, len(received))
        # Each invalid message produced exactly one INVALID_TYPE diagnostic.
        violations = dispatcher.schema_violations
        self.assertEqual(8 * 20, len(violations))
        self.assertTrue(all(v.kind == INVALID_TYPE for v in violations))


class TestAsyncSchemaRouting(unittest.IsolatedAsyncioTestCase):
    async def test_async_mixed_bundle(self):
        dispatcher = Dispatcher()
        received = []

        async def cb(address, *args):
            received.append((address, args))

        dispatcher.map("/light/1", cb, schema=MessageSchema("if"))
        bundle = build_bundle(
            build_message("/light/1", 1, 0.5),
            build_message("/light/1", 1),  # missing
            build_message("/light/1", 1, float("nan")),  # non-finite
        )
        await dispatcher.async_call_handlers_for_packet(bundle.dgram, CLIENT)
        self.assertEqual([("/light/1", (1, 0.5))], received)
        self.assertEqual(
            [MISSING_ARGUMENT, NON_FINITE_VALUE],
            [v.kind for v in dispatcher.schema_violations],
        )

    async def test_async_callback_exception_is_reported(self):
        dispatcher = Dispatcher()

        async def boom(address, *args):
            raise RuntimeError("async device blew up")

        dispatcher.map("/light/1", boom, schema=MessageSchema("i"))
        await dispatcher.async_call_handlers_for_packet(
            build_message("/light/1", 1).dgram, CLIENT
        )
        (violation,) = dispatcher.schema_violations
        self.assertEqual(CALLBACK_ERROR, violation.kind)
        self.assertIsInstance(violation.exception, RuntimeError)


if __name__ == "__main__":
    unittest.main()
