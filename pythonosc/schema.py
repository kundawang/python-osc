"""Optional argument schema validation for OSC address routes.

Lighting consoles and similar devices expect a fixed parameter table per OSC
address. This module lets a :class:`pythonosc.dispatcher.Dispatcher` declare,
per address pattern, how many arguments a message must carry, which OSC types
they must have and which numeric ranges are allowed. Messages that fail
validation never reach the device callback; instead a
:class:`SchemaViolation` diagnostic is reported with the OSC address and the
position of the offending argument.
"""

import dataclasses
import math

from pythonosc.osc_message import OscMessage
from pythonosc.parsing import osc_types

from typing import List, Optional, Sequence, Union

# Kinds of schema violations.
MISSING_ARGUMENT = "missing_argument"
UNEXPECTED_ARGUMENT = "unexpected_argument"
INVALID_TYPE = "invalid_type"
NON_FINITE_VALUE = "non_finite_value"
OUT_OF_RANGE = "out_of_range"
CALLBACK_ERROR = "callback_error"

# Type tags whose values are numeric and may carry a range / finiteness check.
_NUMERIC_TAGS = frozenset("ihfd")
# Type tags this module knows how to validate values for.
_KNOWN_TAGS = frozenset("ihfdsbTFN")


@dataclasses.dataclass(frozen=True)
class SchemaViolation:
    """Diagnostic describing why a message (or its dispatch) failed.

    Attributes:
        address: OSC address of the offending message.
        kind: One of the ``*_ARGUMENT``/``INVALID_TYPE``/``NON_FINITE_VALUE``/
            ``OUT_OF_RANGE``/``CALLBACK_ERROR`` constants.
        index: Position of the offending argument (0-based), or None when the
            violation concerns the message as a whole (e.g. callback errors).
        expected: Human readable description of what the schema expects.
        actual: Human readable description of what was received.
        exception: The exception raised by a callback, for CALLBACK_ERROR.
    """

    address: str
    kind: str
    index: Optional[int] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    exception: Optional[BaseException] = None

    def __str__(self) -> str:
        where = f" argument {self.index}" if self.index is not None else ""
        detail = ""
        if self.expected is not None or self.actual is not None:
            detail = f" (expected {self.expected}, got {self.actual})"
        if self.exception is not None:
            detail = f" ({self.exception!r})"
        return f"{self.address}:{where} {self.kind}{detail}"


@dataclasses.dataclass(frozen=True)
class Param:
    """Schema for a single OSC argument.

    Attributes:
        types: String of acceptable OSC type tags, e.g. ``"f"`` or ``"if"``.
        minimum: Optional lower bound (inclusive) for numeric arguments.
        maximum: Optional upper bound (inclusive) for numeric arguments.
    """

    types: str
    minimum: Optional[float] = None
    maximum: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.types or not isinstance(self.types, str):
            raise ValueError("Param types must be a non-empty string of OSC type tags")
        unknown = set(self.types) - _KNOWN_TAGS
        if unknown:
            raise ValueError(f"Unsupported OSC type tag(s) in schema: {sorted(unknown)}")
        if (self.minimum is not None or self.maximum is not None) and not (
            set(self.types) <= _NUMERIC_TAGS
        ):
            raise ValueError("Ranges only apply to numeric type tags (i, h, f, d)")
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            raise ValueError("minimum must not be greater than maximum")


def _type_tags_of(message: OscMessage) -> str:
    """Extracts the OSC type tag string of a parsed message from its datagram."""
    _, index = osc_types.get_string(message.dgram, 0)
    if not message.dgram[index:]:
        return ""
    type_tag, _ = osc_types.get_string(message.dgram, index)
    if not type_tag.startswith(","):
        return ""
    return type_tag[1:]


class MessageSchema(object):
    """Ordered list of :class:`Param` describing the arguments of an address.

    Can be built from ``Param`` objects or, as a shorthand when no ranges are
    needed, from a plain type tag string::

        MessageSchema([Param("i", minimum=0, maximum=127), Param("f")])
        MessageSchema("ifs")
    """

    def __init__(self, params: Union[str, Sequence[Param]]) -> None:
        if isinstance(params, str):
            params = [Param(tag) for tag in params]
        self._params = tuple(params)
        for param in self._params:
            if not isinstance(param, Param):
                raise ValueError(f"Expected Param instances, got {param!r}")

    @property
    def params(self) -> tuple:
        """Returns the tuple of :class:`Param` of this schema."""
        return self._params

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MessageSchema) and self._params == other._params

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __repr__(self) -> str:
        return f"MessageSchema({list(self._params)!r})"

    def validate(self, message: OscMessage) -> List[SchemaViolation]:
        """Validates a parsed message against this schema.

        Args:
            message: The parsed OSC message to check.

        Returns:
            A list of :class:`SchemaViolation`, empty when the message is
            legal. Every violation carries the message address and, for
            argument-level problems, the 0-based argument position.
        """
        address = message.address
        tags = _type_tags_of(message)
        values = message.params
        violations = []  # type: List[SchemaViolation]
        expected_count = len(self._params)

        if len(tags) < expected_count:
            violations.append(
                SchemaViolation(
                    address=address,
                    kind=MISSING_ARGUMENT,
                    index=len(tags),
                    expected=f"{expected_count} argument(s)",
                    actual=f"{len(tags)} argument(s)",
                )
            )
        elif len(tags) > expected_count:
            violations.append(
                SchemaViolation(
                    address=address,
                    kind=UNEXPECTED_ARGUMENT,
                    index=expected_count,
                    expected=f"{expected_count} argument(s)",
                    actual=f"{len(tags)} argument(s)",
                )
            )

        for index, (param, tag) in enumerate(zip(self._params, tags)):
            value = values[index] if index < len(values) else None
            if tag not in param.types:
                violations.append(
                    SchemaViolation(
                        address=address,
                        kind=INVALID_TYPE,
                        index=index,
                        expected=f"type '{param.types}'",
                        actual=f"type '{tag}'",
                    )
                )
                continue
            if tag in _NUMERIC_TAGS and isinstance(value, (int, float)):
                if isinstance(value, float) and not math.isfinite(value):
                    violations.append(
                        SchemaViolation(
                            address=address,
                            kind=NON_FINITE_VALUE,
                            index=index,
                            expected="a finite number",
                            actual=repr(value),
                        )
                    )
                    continue
                if param.minimum is not None and value < param.minimum:
                    violations.append(
                        SchemaViolation(
                            address=address,
                            kind=OUT_OF_RANGE,
                            index=index,
                            expected=f">= {param.minimum}",
                            actual=repr(value),
                        )
                    )
                elif param.maximum is not None and value > param.maximum:
                    violations.append(
                        SchemaViolation(
                            address=address,
                            kind=OUT_OF_RANGE,
                            index=index,
                            expected=f"<= {param.maximum}",
                            actual=repr(value),
                        )
                    )
        return violations
