"""Split OSC bundles into multiple bounded datagrams.

A single OSC bundle holding hundreds of updates can exceed the path's UDP
datagram limit (MTU) and get dropped by the network.  The functions in this
module split the elements of one timetag into several *legal* OSC bundles,
each of which fits within a caller supplied maximum datagram size.

Guarantees:
  * elements are never split: an individual message or a nested bundle is
    always emitted atomically, nested bundles keep their inner timetag and
    contents,
  * element order is preserved across the emitted bundles,
  * every emitted bundle is a standalone OSC packet that the existing
    :class:`pythonosc.osc_bundle.OscBundle` parser can read back.
"""

from typing import List, Union

from pythonosc import osc_bundle
from pythonosc import osc_bundle_builder
from pythonosc import osc_message

# An element that can be placed inside an OSC bundle.
BundleContent = Union[osc_bundle.OscBundle, osc_message.OscMessage]

# Every OSC bundle starts with the b"#bundle\x00" prefix (8 bytes) followed by
# its 8 byte timetag, so a datagram smaller than this can never hold a bundle.
BUNDLE_HEADER_SIZE = len(b"#bundle\x00") + 8


class BundleBuildError(Exception):
    """Raised when a set of elements cannot be packed into bounded bundles.

    The failure always happens before any datagram is transmitted, hence
    :attr:`sent_count` is always zero.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self._sent_count = 0

    @property
    def sent_count(self) -> int:
        """Datagrams sent before the failure (always 0 for build errors)."""
        return self._sent_count


def split_bundle(
    bundle: osc_bundle.OscBundle, max_datagram_size: int
) -> List[osc_bundle.OscBundle]:
    """Split an existing :class:`OscBundle` into bounded bundles.

    The resulting bundles all carry ``bundle``'s timetag and its elements in
    their original order.  Nested bundles are treated as opaque atomic
    elements: they are never unpacked, so their inner timetag and contents are
    preserved verbatim.

    Args:
      bundle: the bundle whose elements should be distributed.
      max_datagram_size: maximum size in bytes of any emitted datagram.

    Returns:
      a list of :class:`OscBundle` instances, each no larger than
      ``max_datagram_size``.

    Raises:
      BundleBuildError: if ``bundle`` is not an OscBundle, it holds no
        elements, the size limit is smaller than the bundle header, or an
        element does not fit on its own.
    """
    if not isinstance(bundle, osc_bundle.OscBundle):
        raise BundleBuildError(
            f"split_bundle expects an OscBundle, got {type(bundle).__name__}"
        )
    return _pack_bundles(list(bundle), bundle.timestamp, max_datagram_size)


def build_bounded_bundles(
    contents: List[BundleContent],
    timestamp: Union[int, float],
    max_datagram_size: int,
) -> List[osc_bundle.OscBundle]:
    """Pack ``contents`` sharing one timetag into bounded OSC bundles.

    Elements are packed in order with a greedy first-fit strategy: an element
    is appended to the current bundle while it fits, otherwise the current
    bundle is sealed and a new one carrying the same timetag is started.

    Every nested bundle in ``contents`` must carry ``timestamp`` itself; a
    nested bundle with a different timetag is refused rather than silently
    re-scheduled, because wrapping it under another timetag would change when
    its messages fire.

    Args:
      contents: ordered messages and/or nested bundles for one timetag.
      timestamp: the timetag shared by every emitted bundle (seconds since the
        epoch in UTC, or :data:`IMMEDIATELY`).
      max_datagram_size: maximum size in bytes of any emitted datagram.

    Returns:
      a list of :class:`OscBundle` instances, each no larger than
      ``max_datagram_size``.

    Raises:
      BundleBuildError: if ``contents`` is empty, the size limit is smaller
        than the bundle header, an element is not a message or bundle, a
        nested bundle uses a different timetag, or a single element does not
        fit within ``max_datagram_size``.
    """
    for content in contents:
        if isinstance(content, osc_bundle.OscBundle) and content.timestamp != timestamp:
            raise BundleBuildError(
                "Nested bundle timetag "
                f"{content.timestamp!r} does not match the outer timetag "
                f"{timestamp!r}"
            )
    return _pack_bundles(contents, timestamp, max_datagram_size)


def _pack_bundles(
    contents: List[BundleContent],
    timestamp: Union[int, float],
    max_datagram_size: int,
) -> List[osc_bundle.OscBundle]:
    """Validate and greedily pack elements into bounded bundles."""
    if not isinstance(max_datagram_size, int) or isinstance(max_datagram_size, bool):
        raise BundleBuildError(
            "max_datagram_size must be an integer, got "
            f"{type(max_datagram_size).__name__}"
        )
    if max_datagram_size < BUNDLE_HEADER_SIZE:
        raise BundleBuildError(
            "max_datagram_size must be at least "
            f"{BUNDLE_HEADER_SIZE} bytes (the OSC bundle header), got "
            f"{max_datagram_size}"
        )
    if not contents:
        raise BundleBuildError("Cannot build bounded bundles from empty contents")

    # Validate every element up front so that an error is raised before any
    # bundle is built, instead of leaving a partial result behind.
    for content in contents:
        if not isinstance(content, (osc_message.OscMessage, osc_bundle.OscBundle)):
            raise BundleBuildError(
                "Content must be either OscBundle or OscMessage, found "
                f"{type(content).__name__}"
            )
        # An element is stored as a 4 byte size prefix followed by its
        # datagram; it must fit inside a bundle on its own.
        if BUNDLE_HEADER_SIZE + 4 + content.size > max_datagram_size:
            raise BundleBuildError(
                f"Element of size {content.size} bytes does not fit within "
                f"max_datagram_size of {max_datagram_size} bytes"
            )

    bundles: List[osc_bundle.OscBundle] = []
    builder = osc_bundle_builder.OscBundleBuilder(timestamp)
    current_size = BUNDLE_HEADER_SIZE

    for content in contents:
        element_size = 4 + content.size
        if current_size + element_size > max_datagram_size:
            bundles.append(builder.build())
            builder = osc_bundle_builder.OscBundleBuilder(timestamp)
            current_size = BUNDLE_HEADER_SIZE
        builder.add_content(content)
        current_size += element_size

    bundles.append(builder.build())
    return bundles
