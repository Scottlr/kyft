"""Recorded window models and direct history query APIs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from spanfold.temporal import TemporalAxis, TemporalPoint, TemporalRange, TemporalRangeEndStatus


class WindowTransitionKind(Enum):
    """Describes whether a window transition opened or closed a window."""

    OPENED = "opened"
    CLOSED = "closed"


class WindowBoundaryReason(Enum):
    """Describes why a window boundary was emitted."""

    ACTIVE_PREDICATE_ENDED = "active_predicate_ended"
    SEGMENT_CHANGED = "segment_changed"


class WindowGroupKind(Enum):
    """Identifies the metadata family used to group recorded windows."""

    SEGMENT = "segment"
    TAG = "tag"


@dataclass(frozen=True, slots=True)
class WindowSegment:
    """Analytical segment value attached to a window."""

    name: str
    value: Any
    parent_name: str | None = None


@dataclass(frozen=True, slots=True)
class WindowTag:
    """Descriptive metadata attached to a window."""

    name: str
    value: Any


@dataclass(frozen=True, slots=True)
class WindowBoundaryChange:
    """A segment value change that caused a window boundary."""

    name: str
    previous_value: Any
    current_value: Any


@dataclass(frozen=True, slots=True)
class WindowRecordId:
    """Stable deterministic identity for a recorded window replay."""

    value: str

    @classmethod
    def from_window(cls, window: WindowRecord) -> WindowRecordId:
        """Create an identity from stable window fields."""

        payload = {
            "window_name": window.window_name,
            "key": repr(window.key),
            "start_position": window.start_position,
            "end_position": window.end_position,
            "source": repr(window.source),
            "partition": repr(window.partition),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return cls(hashlib.sha1(raw).hexdigest()[:16])

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class WindowRecord:
    """Common shape of an open or closed recorded window."""

    window_name: str
    key: Any
    start_position: int
    end_position: int | None = None
    source: Any = None
    partition: Any = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    segments: tuple[WindowSegment, ...] = field(default_factory=tuple)
    tags: tuple[WindowTag, ...] = field(default_factory=tuple)
    boundary_reason: WindowBoundaryReason | None = None
    boundary_changes: tuple[WindowBoundaryChange, ...] = field(default_factory=tuple)

    @property
    def id(self) -> WindowRecordId:
        """Return the deterministic identity for this recorded window."""

        return WindowRecordId.from_window(self)

    @property
    def is_closed(self) -> bool:
        """Return whether this window has a recorded end position."""

        return self.end_position is not None

    def range_for_axis(
        self,
        axis: TemporalAxis = TemporalAxis.PROCESSING_POSITION,
        *,
        horizon: TemporalPoint | None = None,
    ) -> TemporalRange | None:
        """Return this window as a temporal range.

        Open windows return ``None`` unless a comparable horizon is supplied.
        """

        if axis is TemporalAxis.PROCESSING_POSITION:
            start = TemporalPoint.for_position(self.start_position)
            if self.end_position is not None:
                return TemporalRange.closed(start, TemporalPoint.for_position(self.end_position))
        elif axis is TemporalAxis.TIMESTAMP:
            if self.start_time is None:
                return None
            start = TemporalPoint.for_timestamp(self.start_time, "event-time")
            if self.end_time is not None:
                return TemporalRange.closed(
                    start,
                    TemporalPoint.for_timestamp(self.end_time, "event-time"),
                )
        else:
            return None

        if horizon is None:
            return None
        return TemporalRange.with_effective_end(
            start,
            horizon,
            TemporalRangeEndStatus.OPEN_AT_HORIZON,
        )


@dataclass(frozen=True, init=False, slots=True)
class ClosedWindow(WindowRecord):
    """A closed recorded window."""

    def __init__(
        self,
        window_name: str,
        key: Any,
        start_position: int,
        end_position: int,
        source: Any = None,
        partition: Any = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        segments: Iterable[WindowSegment] | None = None,
        tags: Iterable[WindowTag] | None = None,
        boundary_reason: WindowBoundaryReason | None = None,
        boundary_changes: Iterable[WindowBoundaryChange] | None = None,
    ) -> None:
        WindowRecord.__init__(
            self,
            window_name,
            key,
            start_position,
            end_position,
            source,
            partition,
            start_time,
            end_time,
            tuple(segments or ()),
            tuple(tags or ()),
            boundary_reason,
            tuple(boundary_changes or ()),
        )


@dataclass(frozen=True, init=False, slots=True)
class OpenWindow(WindowRecord):
    """A currently open recorded window."""

    def __init__(
        self,
        window_name: str,
        key: Any,
        start_position: int,
        source: Any = None,
        partition: Any = None,
        start_time: datetime | None = None,
        segments: Iterable[WindowSegment] | None = None,
        tags: Iterable[WindowTag] | None = None,
    ) -> None:
        WindowRecord.__init__(
            self,
            window_name,
            key,
            start_position,
            None,
            source,
            partition,
            start_time,
            None,
            tuple(segments or ()),
            tuple(tags or ()),
            None,
            (),
        )


@dataclass(frozen=True, slots=True)
class WindowAnnotationTarget:
    """Stable target identity used to attach annotations to a window."""

    window_name: str
    key: Any
    start_position: int
    source: Any = None
    partition: Any = None

    @classmethod
    def from_window(cls, window: WindowRecord) -> WindowAnnotationTarget:
        """Create an annotation target from a recorded window."""

        return cls(
            window.window_name,
            window.key,
            window.start_position,
            window.source,
            window.partition,
        )


@dataclass(frozen=True, slots=True)
class WindowAnnotation:
    """Append-only metadata attached to a recorded window target."""

    target: WindowAnnotationTarget
    name: str
    value: Any
    known_at: TemporalPoint | None = None
    revision: int = 1


@dataclass(frozen=True, slots=True)
class WindowGroupSummary:
    """Summarizes recorded windows sharing one segment or tag value."""

    group_kind: WindowGroupKind
    name: str
    value: Any
    record_count: int
    final_count: int
    provisional_count: int
    measured_position_count: int
    total_position_length: int
    measured_time_count: int
    total_time_duration: timedelta


@dataclass(frozen=True, slots=True)
class WindowHistorySnapshot:
    """A read-only view of recorded history at a horizon."""

    horizon: TemporalPoint
    windows: tuple[WindowRecord, ...]

    @property
    def closed_windows(self) -> tuple[ClosedWindow, ...]:
        """Return closed windows visible in the snapshot."""

        return tuple(window for window in self.windows if isinstance(window, ClosedWindow))

    @property
    def open_windows(self) -> tuple[WindowRecord, ...]:
        """Return horizon-clipped windows that were still open."""

        return tuple(window for window in self.windows if not isinstance(window, ClosedWindow))

    def query(self) -> WindowHistoryQuery:
        """Start a direct read-only query over snapshot windows."""

        return WindowHistoryQuery(self.windows)


class WindowHistory:
    """Stores recorded open and closed windows and exposes window queries."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self._open: dict[tuple[str, Any, Any, Any, tuple[WindowSegment, ...]], OpenWindow] = {}
        self._closed: list[ClosedWindow] = []
        self._annotations: list[WindowAnnotation] = []

    @property
    def closed_windows(self) -> tuple[ClosedWindow, ...]:
        """Return closed windows recorded by the pipeline."""

        return tuple(self._closed)

    @property
    def open_windows(self) -> tuple[OpenWindow, ...]:
        """Return currently open windows recorded by the pipeline."""

        return tuple(self._open.values())

    @property
    def windows(self) -> tuple[WindowRecord, ...]:
        """Return all recorded windows, including currently open windows."""

        return (*self.closed_windows, *self.open_windows)

    @property
    def annotations(self) -> tuple[WindowAnnotation, ...]:
        """Return annotations attached to recorded windows."""

        return tuple(self._annotations)

    def record_open(self, window: OpenWindow) -> None:
        """Record an opened window."""

        if not self.enabled:
            return
        self._open[_recording_key(window)] = window

    def record_close(
        self,
        *,
        window_name: str,
        key: Any,
        end_position: int,
        source: Any = None,
        partition: Any = None,
        end_time: datetime | None = None,
        boundary_reason: WindowBoundaryReason | None = None,
        boundary_changes: Iterable[WindowBoundaryChange] | None = None,
    ) -> ClosedWindow | None:
        """Close and record a matching open window."""

        if not self.enabled:
            return None
        open_key = next(
            (
                candidate
                for candidate in self._open
                if candidate[:4] == (window_name, key, source, partition)
            ),
            None,
        )
        open_window = self._open.pop(open_key, None) if open_key is not None else None
        if open_window is None:
            return None
        closed = ClosedWindow(
            window_name,
            key,
            open_window.start_position,
            end_position,
            source,
            partition,
            open_window.start_time,
            end_time,
            open_window.segments,
            open_window.tags,
            boundary_reason,
            boundary_changes,
        )
        self._closed.append(closed)
        return closed

    def query(self) -> WindowHistoryQuery:
        """Start a direct read-only query over recorded windows."""

        return WindowHistoryQuery(self.windows)

    def for_window(self, window_name: str) -> tuple[WindowRecord, ...]:
        """Return recorded windows for a configured window name."""

        return self.query().where_window(window_name).all()

    def with_segment(self, name: str, value: Any) -> tuple[WindowRecord, ...]:
        """Return windows containing a required segment value."""

        return self.query().where_segment(name, value).all()

    def with_tag(self, name: str, value: Any) -> tuple[WindowRecord, ...]:
        """Return windows containing a required tag value."""

        return self.query().where_tag(name, value).all()

    def snapshot_at(self, horizon: TemporalPoint) -> WindowHistorySnapshot:
        """Evaluate recorded windows at an explicit horizon."""

        windows: list[WindowRecord] = list(self._closed)
        for window in self._open.values():
            if horizon.axis is TemporalAxis.PROCESSING_POSITION:
                windows.append(
                    WindowRecord(
                        window.window_name,
                        window.key,
                        window.start_position,
                        horizon.position,
                        window.source,
                        window.partition,
                        window.start_time,
                        window.end_time,
                        window.segments,
                        window.tags,
                    )
                )
            else:
                windows.append(window)
        return WindowHistorySnapshot(horizon, tuple(windows))

    def annotate(
        self,
        window: WindowRecord | WindowAnnotationTarget,
        name: str,
        value: Any,
        known_at: TemporalPoint | None = None,
    ) -> WindowAnnotation:
        """Attach append-only metadata to a recorded window."""

        target = (
            window
            if isinstance(window, WindowAnnotationTarget)
            else WindowAnnotationTarget.from_window(window)
        )
        if known_at is not None and known_at.axis is TemporalAxis.UNKNOWN:
            msg = "Annotation known-at point must use a known temporal axis."
            raise ValueError(msg)
        revision = 1 + sum(
            1
            for annotation in self._annotations
            if annotation.target == target and annotation.name == name
        )
        annotation = WindowAnnotation(target, name, value, known_at, revision)
        self._annotations.append(annotation)
        return annotation

    def annotations_for(
        self,
        window: WindowRecord | WindowAnnotationTarget,
    ) -> tuple[WindowAnnotation, ...]:
        """Return annotations attached to a recorded window or target."""

        target = (
            window
            if isinstance(window, WindowAnnotationTarget)
            else WindowAnnotationTarget.from_window(window)
        )
        return tuple(annotation for annotation in self._annotations if annotation.target == target)

    def annotations_known_at(
        self,
        window: WindowRecord | WindowAnnotationTarget,
        horizon: TemporalPoint,
    ) -> tuple[WindowAnnotation, ...]:
        """Return annotations whose known-at point is at or before a horizon."""

        target = (
            window
            if isinstance(window, WindowAnnotationTarget)
            else WindowAnnotationTarget.from_window(window)
        )
        return tuple(
            annotation
            for annotation in self._annotations
            if annotation.target == target
            and annotation.known_at is not None
            and annotation.known_at.axis is horizon.axis
            and annotation.known_at <= horizon
        )

    def compare(self, name: str) -> Any:
        """Start a staged comparison over this recorded history."""

        from spanfold.comparison import WindowComparisonBuilder

        return WindowComparisonBuilder(self, name)

    def compare_sources(
        self,
        name: str,
        window_name: str,
        sources: Iterable[Any],
    ) -> Any:
        """Build a directional pairwise matrix over recorded window sources."""

        from spanfold.comparison import build_source_matrix

        return build_source_matrix(self, name, window_name, sources)

    def compare_hierarchy(
        self,
        name: str,
        parent_window_name: str,
        child_window_name: str,
    ) -> Any:
        """Compare parent windows against child contribution windows."""

        from spanfold.comparison import build_hierarchy_comparison

        return build_hierarchy_comparison(
            self,
            name,
            parent_window_name,
            child_window_name,
        )


class WindowHistoryQuery:
    """Fluent direct query API for recorded windows."""

    def __init__(self, windows: Iterable[WindowRecord]) -> None:
        self._windows = tuple(windows)

    def where_window(self, window_name: str) -> WindowHistoryQuery:
        """Filter by configured window name."""

        return self._filter(lambda window: window.window_name == window_name)

    def where_key(self, key: Any) -> WindowHistoryQuery:
        """Filter by logical window key."""

        return self._filter(lambda window: window.key == key)

    def where_source(self, source: Any) -> WindowHistoryQuery:
        """Filter by source or lane."""

        return self._filter(lambda window: window.source == source)

    def where_lane(self, lane: Any) -> WindowHistoryQuery:
        """Alias for filtering by source/lane."""

        return self.where_source(lane)

    def where_partition(self, partition: Any) -> WindowHistoryQuery:
        """Filter by partition."""

        return self._filter(lambda window: window.partition == partition)

    def where_segment(self, name: str, value: Any) -> WindowHistoryQuery:
        """Filter by segment value."""

        return self._filter(
            lambda window: any(
                segment.name == name and segment.value == value for segment in window.segments
            )
        )

    def where_tag(self, name: str, value: Any) -> WindowHistoryQuery:
        """Filter by tag value."""

        return self._filter(
            lambda window: any(tag.name == name and tag.value == value for tag in window.tags)
        )

    def closed(self) -> WindowHistoryQuery:
        """Return only closed windows."""

        return self._filter(lambda window: window.is_closed)

    def open(self) -> WindowHistoryQuery:
        """Return only currently open windows."""

        return self._filter(lambda window: not window.is_closed)

    def latest(self) -> WindowRecord | None:
        """Return the latest window by start position, or ``None``."""

        return max(self._windows, key=lambda window: window.start_position, default=None)

    def all(self) -> tuple[WindowRecord, ...]:
        """Materialize the query result."""

        return self._windows

    def summarize_by_segment(self, name: str) -> tuple[WindowGroupSummary, ...]:
        """Summarize matching windows by one segment dimension."""

        return summarize_by_segment(self._windows, name)

    def summarize_by_tag(self, name: str) -> tuple[WindowGroupSummary, ...]:
        """Summarize matching windows by one tag."""

        return summarize_by_tag(self._windows, name)

    def _filter(self, predicate: Any) -> WindowHistoryQuery:
        return WindowHistoryQuery(window for window in self._windows if predicate(window))


def summarize_by_segment(
    windows: Iterable[WindowRecord],
    name: str,
) -> tuple[WindowGroupSummary, ...]:
    """Summarize recorded windows by one segment dimension."""

    return _summarize_by_metadata(windows, WindowGroupKind.SEGMENT, name)


def summarize_by_tag(
    windows: Iterable[WindowRecord],
    name: str,
) -> tuple[WindowGroupSummary, ...]:
    """Summarize recorded windows by one tag."""

    return _summarize_by_metadata(windows, WindowGroupKind.TAG, name)


def coerce_segments(
    values: Mapping[str, Any] | Iterable[WindowSegment] | None,
) -> tuple[WindowSegment, ...]:
    """Coerce user segment results into immutable segment records."""

    if values is None:
        return ()
    if isinstance(values, Mapping):
        return tuple(WindowSegment(name, value) for name, value in values.items())
    return tuple(values)


def coerce_tags(values: Mapping[str, Any] | Iterable[WindowTag] | None) -> tuple[WindowTag, ...]:
    """Coerce user tag results into immutable tag records."""

    if values is None:
        return ()
    if isinstance(values, Mapping):
        return tuple(WindowTag(name, value) for name, value in values.items())
    return tuple(values)


@dataclass(slots=True)
class _WindowSummaryAccumulator:
    group_kind: WindowGroupKind
    name: str
    value: Any
    record_count: int = 0
    final_count: int = 0
    provisional_count: int = 0
    measured_position_count: int = 0
    total_position_length: int = 0
    measured_time_count: int = 0
    total_time_duration: timedelta = timedelta()

    def add(self, window: WindowRecord) -> None:
        self.record_count += 1
        if isinstance(window, ClosedWindow):
            self.final_count += 1
        else:
            self.provisional_count += 1

        if window.end_position is not None:
            self.measured_position_count += 1
            self.total_position_length += window.end_position - window.start_position

        if window.start_time is not None and window.end_time is not None:
            self.measured_time_count += 1
            self.total_time_duration += window.end_time - window.start_time

    def to_summary(self) -> WindowGroupSummary:
        return WindowGroupSummary(
            self.group_kind,
            self.name,
            self.value,
            self.record_count,
            self.final_count,
            self.provisional_count,
            self.measured_position_count,
            self.total_position_length,
            self.measured_time_count,
            self.total_time_duration,
        )


def _summarize_by_metadata(
    windows: Iterable[WindowRecord],
    group_kind: WindowGroupKind,
    name: str,
) -> tuple[WindowGroupSummary, ...]:
    if not name or not name.strip():
        msg = "Summary dimension name cannot be empty."
        raise ValueError(msg)

    groups: dict[tuple[str, str], _WindowSummaryAccumulator] = {}
    for window in windows:
        for value in _metadata_values(window, group_kind, name):
            key = (type(value).__qualname__, repr(value))
            accumulator = groups.get(key)
            if accumulator is None:
                accumulator = _WindowSummaryAccumulator(group_kind, name, value)
                groups[key] = accumulator
            accumulator.add(window)

    return tuple(
        accumulator.to_summary()
        for _, accumulator in sorted(groups.items(), key=lambda item: item[0])
    )


def _metadata_values(
    window: WindowRecord,
    group_kind: WindowGroupKind,
    name: str,
) -> tuple[Any, ...]:
    values: list[Any] = []
    metadata: Iterable[WindowSegment | WindowTag] = (
        window.segments if group_kind is WindowGroupKind.SEGMENT else window.tags
    )
    for item in metadata:
        if item.name != name or item.value in values:
            continue
        values.append(item.value)
    return tuple(values)


def _recording_key(window: WindowRecord) -> tuple[str, Any, Any, Any, tuple[WindowSegment, ...]]:
    return (window.window_name, window.key, window.source, window.partition, window.segments)
