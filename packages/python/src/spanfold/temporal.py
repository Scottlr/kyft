"""Temporal primitives for ordering and aligning recorded windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import total_ordering


class TemporalAxis(Enum):
    """Identifies the axis used to order a temporal point."""

    UNKNOWN = "unknown"
    PROCESSING_POSITION = "processing_position"
    TIMESTAMP = "timestamp"


class TemporalRangeEndStatus(Enum):
    """Describes how a temporal range end was established."""

    UNKNOWN = "unknown"
    CLOSED = "closed"
    UNKNOWN_END = "unknown_end"
    OPEN_AT_HORIZON = "open_at_horizon"
    CLIPPED_BY_QUERY_RANGE = "clipped_by_query_range"


@total_ordering
@dataclass(frozen=True, slots=True)
class TemporalPoint:
    """A single point on one temporal axis.

    Processing-position points compare by integer ingestion position. Timestamp
    points compare by ``datetime`` value only when their optional clock labels
    match. Points on different axes are intentionally not comparable.
    """

    axis: TemporalAxis
    _position: int | None = None
    _timestamp: datetime | None = None
    clock: str | None = None

    @classmethod
    def for_position(cls, position: int) -> TemporalPoint:
        """Create a point ordered by pipeline processing position."""

        return cls(TemporalAxis.PROCESSING_POSITION, _position=int(position))

    @classmethod
    def for_timestamp(cls, timestamp: datetime, clock: str | None = None) -> TemporalPoint:
        """Create a point ordered by event timestamp."""

        return cls(TemporalAxis.TIMESTAMP, _timestamp=timestamp, clock=clock)

    @property
    def position(self) -> int:
        """Return the processing position value."""

        if self.axis is not TemporalAxis.PROCESSING_POSITION or self._position is None:
            msg = "Only processing-position points expose a position value."
            raise ValueError(msg)
        return self._position

    @property
    def timestamp(self) -> datetime:
        """Return the timestamp value."""

        if self.axis is not TemporalAxis.TIMESTAMP or self._timestamp is None:
            msg = "Only timestamp points expose a timestamp value."
            raise ValueError(msg)
        return self._timestamp

    def compare_to(self, other: TemporalPoint) -> int:
        """Compare this point with another point on the same axis."""

        self._ensure_comparable(other)
        if self.axis is TemporalAxis.PROCESSING_POSITION:
            return (self.position > other.position) - (self.position < other.position)
        if self.axis is TemporalAxis.TIMESTAMP:
            return (self.timestamp > other.timestamp) - (self.timestamp < other.timestamp)
        msg = "Unknown temporal points are not comparable."
        raise ValueError(msg)

    def is_before(self, other: TemporalPoint) -> bool:
        """Return whether this point is earlier than another comparable point."""

        return self.compare_to(other) < 0

    def is_after(self, other: TemporalPoint) -> bool:
        """Return whether this point is later than another comparable point."""

        return self.compare_to(other) > 0

    def __lt__(self, other: TemporalPoint) -> bool:
        return self.compare_to(other) < 0

    def _ensure_comparable(self, other: TemporalPoint) -> None:
        if self.axis is TemporalAxis.UNKNOWN or other.axis is TemporalAxis.UNKNOWN:
            msg = "Unknown temporal points are not comparable."
            raise ValueError(msg)
        if self.axis is not other.axis:
            msg = "Temporal points on different axes are not comparable."
            raise ValueError(msg)
        if self.axis is TemporalAxis.TIMESTAMP and self.clock != other.clock:
            msg = "Timestamp points with different clock identities are not comparable."
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TemporalRange:
    """A half-open temporal range.

    The start point is included and the end point is excluded. Open ranges must
    be clipped to an effective end before duration or overlap calculations.
    """

    start: TemporalPoint
    end: TemporalPoint | None
    end_status: TemporalRangeEndStatus

    def __post_init__(self) -> None:
        if self.end is None:
            return
        _ensure_comparable(self.start, self.end)
        if self.end < self.start:
            msg = "Temporal range end cannot be earlier than the start."
            raise ValueError(msg)

    @classmethod
    def closed(cls, start: TemporalPoint, end: TemporalPoint) -> TemporalRange:
        """Create a closed half-open range."""

        return cls(start, end, TemporalRangeEndStatus.CLOSED)

    @classmethod
    def open(cls, start: TemporalPoint) -> TemporalRange:
        """Create an open range with no effective end."""

        return cls(start, None, TemporalRangeEndStatus.UNKNOWN_END)

    @classmethod
    def with_effective_end(
        cls,
        start: TemporalPoint,
        end: TemporalPoint,
        end_status: TemporalRangeEndStatus,
    ) -> TemporalRange:
        """Create a range whose effective end was produced by analysis policy."""

        if end_status in {
            TemporalRangeEndStatus.UNKNOWN,
            TemporalRangeEndStatus.UNKNOWN_END,
            TemporalRangeEndStatus.CLOSED,
        }:
            msg = "Effective ranges must use a clipping or horizon end status."
            raise ValueError(msg)
        return cls(start, end, end_status)

    @property
    def axis(self) -> TemporalAxis:
        """Return the temporal axis shared by the range bounds."""

        return self.start.axis

    @property
    def has_end(self) -> bool:
        """Return whether this range has an effective end point."""

        return self.end is not None

    @property
    def is_closed(self) -> bool:
        """Return whether this range came from a recorded closed window."""

        return self.end_status is TemporalRangeEndStatus.CLOSED

    @property
    def is_empty(self) -> bool:
        """Return whether the range has the same start and end point."""

        return self.require_end() == self.start

    def require_end(self) -> TemporalPoint:
        """Return the effective end or raise if the range is open."""

        if self.end is None:
            msg = (
                "Temporal ranges without an effective end cannot be used for duration "
                "or overlap calculations."
            )
            raise ValueError(msg)
        return self.end

    def position_length(self) -> int:
        """Return the number of processing positions covered by this range."""

        end = self.require_end()
        if self.axis is not TemporalAxis.PROCESSING_POSITION:
            msg = "Only processing-position ranges expose a position length."
            raise ValueError(msg)
        return end.position - self.start.position

    def time_duration(self) -> timedelta:
        """Return the timestamp duration covered by this range."""

        end = self.require_end()
        if self.axis is not TemporalAxis.TIMESTAMP:
            msg = "Only timestamp ranges expose a time duration."
            raise ValueError(msg)
        return end.timestamp - self.start.timestamp

    def magnitude(self) -> float:
        """Return range length as positions or seconds, depending on axis."""

        if self.axis is TemporalAxis.PROCESSING_POSITION:
            return float(self.position_length())
        if self.axis is TemporalAxis.TIMESTAMP:
            return self.time_duration().total_seconds()
        msg = "Unknown temporal ranges do not expose magnitude."
        raise ValueError(msg)

    def overlaps(self, other: TemporalRange) -> bool:
        """Return whether this range overlaps another half-open range."""

        this_end = self.require_end()
        other_end = other.require_end()
        _ensure_comparable(self.start, other.start)
        return self.start < other_end and other.start < this_end

    def contains(self, point: TemporalPoint) -> bool:
        """Return whether a point is inside the half-open range."""

        end = self.require_end()
        _ensure_comparable(self.start, point)
        return self.start <= point < end

    def intersection(self, other: TemporalRange) -> TemporalRange | None:
        """Return the half-open intersection of two ranges, or ``None``."""

        if not self.overlaps(other):
            return None
        start = max(self.start, other.start)
        end = min(self.require_end(), other.require_end())
        status = (
            TemporalRangeEndStatus.CLOSED
            if self.is_closed and other.is_closed
            else TemporalRangeEndStatus.CLIPPED_BY_QUERY_RANGE
        )
        return TemporalRange(start, end, status)

    def residual(self, coverage: list[TemporalRange]) -> list[TemporalRange]:
        """Return portions of this range not covered by the supplied ranges."""

        segments = [self]
        for covered in sorted(coverage, key=lambda item: item.start):
            next_segments: list[TemporalRange] = []
            for segment in segments:
                next_segments.extend(_subtract_one(segment, covered))
            segments = next_segments
            if not segments:
                break
        return segments


def _subtract_one(base: TemporalRange, cut: TemporalRange) -> list[TemporalRange]:
    if not base.overlaps(cut):
        return [base]

    base_end = base.require_end()
    cut_end = cut.require_end()
    ranges: list[TemporalRange] = []
    if base.start < cut.start:
        ranges.append(TemporalRange(base.start, min(cut.start, base_end), base.end_status))
    if cut_end < base_end:
        ranges.append(TemporalRange(max(cut_end, base.start), base_end, base.end_status))
    return [item for item in ranges if not item.is_empty]


def _ensure_comparable(first: TemporalPoint, second: TemporalPoint) -> None:
    first.compare_to(second)
