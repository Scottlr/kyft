"""Event ingestion pipeline for recording temporal state windows."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from spanfold.records import (
    EventPipelineMetadata,
    OpenWindow,
    WindowBoundaryChange,
    WindowBoundaryReason,
    WindowHistory,
    WindowMetadata,
    WindowSegment,
    WindowTag,
    WindowTransitionKind,
    coerce_segments,
    coerce_tags,
)

Selector = Callable[[Any], Any]
Predicate = Callable[[Any], bool]
SegmentSelector = Callable[[Any], Mapping[str, Any] | Iterable[WindowSegment] | None]
TagSelector = Callable[[Any], Mapping[str, Any] | Iterable[WindowTag] | None]
RollUpPredicate = Callable[["ChildActivityView"], bool]
SegmentTransform = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class ChildActivityView:
    """Snapshot of known child activity for a rollup parent."""

    active_count: int
    total_count: int

    def all_active(self) -> bool:
        """Return whether every known child is active."""

        return self.total_count > 0 and self.active_count == self.total_count

    def any_active(self) -> bool:
        """Return whether at least one known child is active."""

        return self.active_count > 0


@dataclass(frozen=True, slots=True)
class WindowEmission:
    """A window open or close transition produced by ingestion."""

    window_name: str
    key: Any
    event: Any
    kind: WindowTransitionKind
    source: Any = None
    partition: Any = None
    segments: tuple[WindowSegment, ...] = ()
    tags: tuple[WindowTag, ...] = ()
    boundary_reason: WindowBoundaryReason | None = None
    boundary_changes: tuple[WindowBoundaryChange, ...] = ()


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """The emissions produced by one or more ingested events."""

    emissions: tuple[WindowEmission, ...]

    @property
    def has_emissions(self) -> bool:
        """Return whether ingestion produced transitions."""

        return bool(self.emissions)


class WindowOptions:
    """Configures optional callbacks for one source window or rollup."""

    def __init__(self) -> None:
        self._opened: list[Callable[[WindowEmission], None]] = []
        self._closed: list[Callable[[WindowEmission], None]] = []

    def on_opened(self, callback: Callable[[WindowEmission], None]) -> WindowOptions:
        """Register a callback invoked when this window opens."""

        if callback is None:
            msg = "Opened callback is required."
            raise ValueError(msg)
        self._opened.append(callback)
        return self

    def on_closed(self, callback: Callable[[WindowEmission], None]) -> WindowOptions:
        """Register a callback invoked when this window closes."""

        if callback is None:
            msg = "Closed callback is required."
            raise ValueError(msg)
        self._closed.append(callback)
        return self

    @property
    def opened_callbacks(self) -> tuple[Callable[[WindowEmission], None], ...]:
        """Return configured open callbacks."""

        return tuple(self._opened)

    @property
    def closed_callbacks(self) -> tuple[Callable[[WindowEmission], None], ...]:
        """Return configured close callbacks."""

        return tuple(self._closed)


@dataclass(slots=True)
class _RollUpDefinition:
    name: str
    key: Selector
    is_active: RollUpPredicate
    preserve_segments: tuple[str, ...] | None = None
    drop_segments: tuple[str, ...] = ()
    rename_segments: Mapping[str, str] = field(default_factory=dict)
    transform_segments: Mapping[str, SegmentTransform] = field(default_factory=dict)
    rollups: list[_RollUpDefinition] = field(default_factory=list)
    opened_callbacks: tuple[Callable[[WindowEmission], None], ...] = ()
    closed_callbacks: tuple[Callable[[WindowEmission], None], ...] = ()


@dataclass(slots=True)
class _WindowDefinition:
    name: str
    key: Selector
    is_active: Predicate
    segments: SegmentSelector | None = None
    tags: TagSelector | None = None
    rollups: list[_RollUpDefinition] = field(default_factory=list)
    opened_callbacks: tuple[Callable[[WindowEmission], None], ...] = ()
    closed_callbacks: tuple[Callable[[WindowEmission], None], ...] = ()


@dataclass(frozen=True, slots=True)
class _ActiveState:
    segments: tuple[WindowSegment, ...]
    tags: tuple[WindowTag, ...]


@dataclass(slots=True)
class _ParentState:
    children: dict[Any, bool] = field(default_factory=dict)
    is_active: bool = False

    def view(self) -> ChildActivityView:
        """Return child activity counts for predicate evaluation."""

        return ChildActivityView(
            active_count=sum(1 for active in self.children.values() if active),
            total_count=len(self.children),
        )


class EventPipeline:
    """Processes events through configured windows and records history."""

    def __init__(
        self,
        windows: Iterable[_WindowDefinition],
        *,
        record_windows: bool = True,
        event_time: Callable[[Any], datetime] | None = None,
        on_emission: Iterable[Callable[[WindowEmission], None]] = (),
    ) -> None:
        self._windows = tuple(windows)
        self._active: dict[tuple[str, Any, Any, Any], _ActiveState] = {}
        self._rollup_parents: dict[
            tuple[str, Any, Any, Any, tuple[WindowSegment, ...]], _ParentState
        ] = {}
        self._position = 0
        self._event_time = event_time
        self._callbacks = tuple(on_emission)
        self._window_callbacks = {
            name: (opened, closed)
            for name, opened, closed in _window_callback_entries(self._windows)
        }
        self.history = WindowHistory(enabled=record_windows)
        self.metadata = EventPipelineMetadata(
            event_type=None,
            windows=tuple(_window_metadata(window) for window in self._windows),
        )

    @property
    def processing_position(self) -> int:
        """Return the latest assigned processing position."""

        return self._position

    def ingest(self, event: Any, *, source: Any = None, partition: Any = None) -> IngestionResult:
        """Ingest one event with optional source and partition context."""

        self._position += 1
        event_time = self._event_time(event) if self._event_time is not None else None
        emissions: list[WindowEmission] = []

        for definition in self._windows:
            self._ingest_window(definition, event, source, partition, event_time, emissions)

        result = IngestionResult(tuple(emissions))
        for emission in result.emissions:
            opened, closed = self._window_callbacks.get(emission.window_name, ((), ()))
            selected_callbacks = (
                opened
                if emission.kind is WindowTransitionKind.OPENED
                else closed
            )
            for callback in selected_callbacks:
                callback(emission)
            for callback in self._callbacks:
                callback(emission)
        return result

    def ingest_many(
        self,
        events: Iterable[Any],
        *,
        source: Any = None,
        partition: Any = None,
    ) -> IngestionResult:
        """Ingest events sequentially and return all emissions in processing order."""

        emissions: list[WindowEmission] = []
        for event in events:
            emissions.extend(self.ingest(event, source=source, partition=partition).emissions)
        return IngestionResult(tuple(emissions))

    def compare(self, name: str) -> Any:
        """Start a comparison over this pipeline's recorded history."""

        return self.history.compare(name)

    def _ingest_window(
        self,
        definition: _WindowDefinition,
        event: Any,
        source: Any,
        partition: Any,
        event_time: datetime | None,
        emissions: list[WindowEmission],
    ) -> None:
        key = definition.key(event)
        state_key = (definition.name, key, source, partition)
        is_active = bool(definition.is_active(event))
        previous = self._active.get(state_key)
        was_active = previous is not None
        current_segments = _select_segments(definition, event) if is_active else ()
        current_tags = _select_tags(definition, event) if is_active else ()

        if is_active and not was_active:
            self._active[state_key] = _ActiveState(current_segments, current_tags)
            self._emit_transition(
                definition.name,
                key,
                event,
                WindowTransitionKind.OPENED,
                source,
                partition,
                event_time,
                current_segments,
                current_tags,
                emissions,
            )
            self._observe_rollups(
                definition.rollups,
                event,
                source,
                partition,
                event_time,
                key,
                True,
                True,
                current_segments,
                current_tags,
                emissions,
            )
            return

        if not is_active and was_active and previous is not None:
            self._active.pop(state_key)
            self._emit_transition(
                definition.name,
                key,
                event,
                WindowTransitionKind.CLOSED,
                source,
                partition,
                event_time,
                previous.segments,
                previous.tags,
                emissions,
                boundary_reason=WindowBoundaryReason.ACTIVE_PREDICATE_ENDED,
            )
            self._observe_rollups(
                definition.rollups,
                event,
                source,
                partition,
                event_time,
                key,
                False,
                True,
                previous.segments,
                previous.tags,
                emissions,
            )
            return

        if (
            is_active
            and was_active
            and previous is not None
            and previous.segments != current_segments
        ):
            changes = _segment_changes(previous.segments, current_segments)
            self._emit_transition(
                definition.name,
                key,
                event,
                WindowTransitionKind.CLOSED,
                source,
                partition,
                event_time,
                previous.segments,
                previous.tags,
                emissions,
                boundary_reason=WindowBoundaryReason.SEGMENT_CHANGED,
                boundary_changes=changes,
            )
            self._active[state_key] = _ActiveState(current_segments, current_tags)
            self._emit_transition(
                definition.name,
                key,
                event,
                WindowTransitionKind.OPENED,
                source,
                partition,
                event_time,
                current_segments,
                current_tags,
                emissions,
            )
            self._observe_rollup_segment_transition(
                definition.rollups,
                event,
                source,
                partition,
                event_time,
                key,
                previous.segments,
                previous.tags,
                current_segments,
                current_tags,
                emissions,
            )
            return

        observed_state = (
            _ActiveState(current_segments, current_tags)
            if is_active
            else previous or _ActiveState((), ())
        )
        self._observe_rollups(
            definition.rollups,
            event,
            source,
            partition,
            event_time,
            key,
            is_active,
            False,
            observed_state.segments,
            observed_state.tags,
            emissions,
        )

    def _emit_transition(
        self,
        window_name: str,
        key: Any,
        event: Any,
        kind: WindowTransitionKind,
        source: Any,
        partition: Any,
        event_time: datetime | None,
        segments: tuple[WindowSegment, ...],
        tags: tuple[WindowTag, ...],
        emissions: list[WindowEmission],
        *,
        boundary_reason: WindowBoundaryReason | None = None,
        boundary_changes: tuple[WindowBoundaryChange, ...] = (),
    ) -> None:
        emissions.append(
            WindowEmission(
                window_name,
                key,
                event,
                kind,
                source,
                partition,
                segments,
                tags,
                boundary_reason,
                boundary_changes,
            )
        )
        if kind is WindowTransitionKind.OPENED:
            self.history.record_open(
                OpenWindow(
                    window_name,
                    key,
                    self._position,
                    source,
                    partition,
                    event_time,
                    segments,
                    tags,
                )
            )
            return

        self.history.record_close(
            window_name=window_name,
            key=key,
            end_position=self._position,
            source=source,
            partition=partition,
            end_time=event_time,
            boundary_reason=boundary_reason,
            boundary_changes=boundary_changes,
        )

    def _observe_rollups(
        self,
        rollups: Iterable[_RollUpDefinition],
        event: Any,
        source: Any,
        partition: Any,
        event_time: datetime | None,
        child_key: Any,
        child_is_active: bool,
        child_changed: bool,
        segments: tuple[WindowSegment, ...],
        tags: tuple[WindowTag, ...],
        emissions: list[WindowEmission],
    ) -> None:
        for rollup in rollups:
            projected_segments = _project_segments(rollup, segments)
            parent_key = rollup.key(event)
            parent_state = self._rollup_parents.setdefault(
                (rollup.name, parent_key, source, partition, projected_segments),
                _ParentState(),
            )
            parent_state.children[child_key] = child_is_active
            parent_changed = False

            if child_changed:
                parent_is_active = bool(rollup.is_active(parent_state.view()))
                if parent_is_active != parent_state.is_active:
                    parent_state.is_active = parent_is_active
                    parent_changed = True
                    self._emit_transition(
                        rollup.name,
                        parent_key,
                        event,
                        WindowTransitionKind.OPENED
                        if parent_is_active
                        else WindowTransitionKind.CLOSED,
                        source,
                        partition,
                        event_time,
                        projected_segments,
                        tags,
                        emissions,
                        boundary_reason=None
                        if parent_is_active
                        else WindowBoundaryReason.ACTIVE_PREDICATE_ENDED,
                    )

            self._observe_rollups(
                rollup.rollups,
                event,
                source,
                partition,
                event_time,
                parent_key,
                parent_state.is_active,
                parent_changed,
                projected_segments,
                tags,
                emissions,
            )

    def _observe_rollup_segment_transition(
        self,
        rollups: Iterable[_RollUpDefinition],
        event: Any,
        source: Any,
        partition: Any,
        event_time: datetime | None,
        child_key: Any,
        previous_segments: tuple[WindowSegment, ...],
        previous_tags: tuple[WindowTag, ...],
        current_segments: tuple[WindowSegment, ...],
        current_tags: tuple[WindowTag, ...],
        emissions: list[WindowEmission],
    ) -> None:
        for rollup in rollups:
            if _project_segments(rollup, previous_segments) != _project_segments(
                rollup, current_segments
            ):
                self._observe_rollups(
                    (rollup,),
                    event,
                    source,
                    partition,
                    event_time,
                    child_key,
                    False,
                    True,
                    previous_segments,
                    previous_tags,
                    emissions,
                )
                self._observe_rollups(
                    (rollup,),
                    event,
                    source,
                    partition,
                    event_time,
                    child_key,
                    True,
                    True,
                    current_segments,
                    current_tags,
                    emissions,
                )
                continue

            self._observe_rollups(
                (rollup,),
                event,
                source,
                partition,
                event_time,
                child_key,
                True,
                True,
                current_segments,
                current_tags,
                emissions,
            )


class EventPipelineBuilder:
    """Configures windows and options for a Spanfold event pipeline."""

    def __init__(self) -> None:
        self._windows: list[_WindowDefinition] = []
        self._names: set[str] = set()
        self._record_windows = False
        self._event_time: Callable[[Any], datetime] | None = None
        self._callbacks: list[Callable[[WindowEmission], None]] = []
        self._current_node: _WindowDefinition | _RollUpDefinition | None = None

    def record_windows(self) -> EventPipelineBuilder:
        """Enable recording of open and closed windows."""

        self._record_windows = True
        return self

    def with_event_time(self, selector: Callable[[Any], datetime]) -> EventPipelineBuilder:
        """Configure event timestamps for recorded windows."""

        self._event_time = selector
        return self

    def on_emission(self, callback: Callable[[WindowEmission], None]) -> EventPipelineBuilder:
        """Register a callback invoked for each emitted transition."""

        if callback is None:
            msg = "Emission callback is required."
            raise ValueError(msg)
        self._callbacks.append(callback)
        return self

    def window(
        self,
        name: str,
        *,
        key: Selector,
        is_active: Predicate,
        segments: SegmentSelector | None = None,
        tags: TagSelector | None = None,
        configure: Callable[[WindowOptions], None] | None = None,
        on_opened: Callable[[WindowEmission], None] | None = None,
        on_closed: Callable[[WindowEmission], None] | None = None,
    ) -> EventPipelineBuilder:
        """Add a state-driven source window and keep configuring the pipeline."""

        options = _build_window_options(configure, on_opened, on_closed)
        self._current_node = self._add_window(
            name,
            key,
            is_active,
            segments,
            tags,
            options,
        )
        return self

    def roll_up(
        self,
        name: str,
        *,
        key: Selector,
        is_active: RollUpPredicate,
        preserve_segments: Iterable[str] | None = None,
        drop_segments: Iterable[str] = (),
        rename_segments: Mapping[str, str] | None = None,
        transform_segments: Mapping[str, SegmentTransform] | None = None,
        configure: Callable[[WindowOptions], None] | None = None,
        on_opened: Callable[[WindowEmission], None] | None = None,
        on_closed: Callable[[WindowEmission], None] | None = None,
    ) -> EventPipelineBuilder:
        """Add a parent rollup for the current source window or rollup."""

        if self._current_node is None:
            msg = "A source window must be configured before adding a rollup."
            raise ValueError(msg)
        self._validate_name(name)
        options = _build_window_options(configure, on_opened, on_closed)
        definition = _RollUpDefinition(
            name,
            key,
            is_active,
            tuple(preserve_segments) if preserve_segments is not None else None,
            tuple(drop_segments),
            dict(rename_segments or {}),
            dict(transform_segments or {}),
            opened_callbacks=options.opened_callbacks,
            closed_callbacks=options.closed_callbacks,
        )
        self._names.add(name)
        self._current_node.rollups.append(definition)
        self._current_node = definition
        return self

    def track_window(
        self,
        name: str,
        *,
        key: Selector,
        is_active: Predicate,
        segments: SegmentSelector | None = None,
        tags: TagSelector | None = None,
        configure: Callable[[WindowOptions], None] | None = None,
        on_opened: Callable[[WindowEmission], None] | None = None,
        on_closed: Callable[[WindowEmission], None] | None = None,
    ) -> EventPipeline:
        """Build a pipeline for one state-driven source window."""

        options = _build_window_options(configure, on_opened, on_closed)
        self._current_node = self._add_window(name, key, is_active, segments, tags, options)
        return self.build()

    def build(self) -> EventPipeline:
        """Build a pipeline from configured windows and options."""

        return EventPipeline(
            self._windows,
            record_windows=self._record_windows,
            event_time=self._event_time,
            on_emission=self._callbacks,
        )

    def _add_window(
        self,
        name: str,
        key: Selector,
        is_active: Predicate,
        segments: SegmentSelector | None,
        tags: TagSelector | None,
        options: WindowOptions,
    ) -> _WindowDefinition:
        self._validate_name(name)
        definition = _WindowDefinition(
            name,
            key,
            is_active,
            segments,
            tags,
            opened_callbacks=options.opened_callbacks,
            closed_callbacks=options.closed_callbacks,
        )
        self._names.add(name)
        self._windows.append(definition)
        return definition

    def _validate_name(self, name: str) -> None:
        if not name or not name.strip():
            msg = "Window name cannot be empty."
            raise ValueError(msg)
        if name in self._names:
            msg = f"Window name already exists: {name}"
            raise ValueError(msg)


class Spanfold:
    """Entry point for building Spanfold event pipelines."""

    @staticmethod
    def for_events() -> EventPipelineBuilder:
        """Create a builder for arbitrary Python event objects."""

        return EventPipelineBuilder()


def _select_segments(definition: _WindowDefinition, event: Any) -> tuple[WindowSegment, ...]:
    if definition.segments is None:
        return ()
    return coerce_segments(definition.segments(event))


def _select_tags(definition: _WindowDefinition, event: Any) -> tuple[WindowTag, ...]:
    if definition.tags is None:
        return ()
    return coerce_tags(definition.tags(event))


def _build_window_options(
    configure: Callable[[WindowOptions], None] | None,
    on_opened: Callable[[WindowEmission], None] | None,
    on_closed: Callable[[WindowEmission], None] | None,
) -> WindowOptions:
    options = WindowOptions()
    if configure is not None:
        configure(options)
    if on_opened is not None:
        options.on_opened(on_opened)
    if on_closed is not None:
        options.on_closed(on_closed)
    return options


def _window_metadata(definition: _WindowDefinition | _RollUpDefinition) -> WindowMetadata:
    return WindowMetadata(
        definition.name,
        tuple(_window_metadata(rollup) for rollup in definition.rollups),
    )


def _window_callback_entries(
    definitions: Iterable[_WindowDefinition | _RollUpDefinition],
) -> Iterable[
    tuple[
        str,
        tuple[Callable[[WindowEmission], None], ...],
        tuple[Callable[[WindowEmission], None], ...],
    ]
]:
    for definition in definitions:
        yield (
            definition.name,
            tuple(definition.opened_callbacks),
            tuple(definition.closed_callbacks),
        )
        yield from _window_callback_entries(definition.rollups)


def _project_segments(
    definition: _RollUpDefinition,
    segments: tuple[WindowSegment, ...],
) -> tuple[WindowSegment, ...]:
    projected: list[WindowSegment] = []
    seen: set[str] = set()
    preserve = (
        set(definition.preserve_segments)
        if definition.preserve_segments is not None
        else None
    )
    drop = set(definition.drop_segments)

    for segment in segments:
        if preserve is not None and segment.name not in preserve:
            continue
        if segment.name in drop:
            continue
        name = definition.rename_segments.get(segment.name, segment.name)
        value = definition.transform_segments.get(segment.name, lambda item: item)(segment.value)
        if name in seen:
            msg = f"Rollup segment projection produced duplicate segment '{name}'."
            raise ValueError(msg)
        seen.add(name)
        parent_name = (
            definition.rename_segments.get(segment.parent_name, segment.parent_name)
            if segment.parent_name is not None
            else None
        )
        if parent_name is not None and preserve is not None and parent_name not in preserve:
            parent_name = None
        projected.append(WindowSegment(name, value, parent_name))

    return tuple(projected)


def _segment_changes(
    previous: tuple[WindowSegment, ...],
    current: tuple[WindowSegment, ...],
) -> tuple[WindowBoundaryChange, ...]:
    changes: list[WindowBoundaryChange] = []
    for index in range(max(len(previous), len(current))):
        before = previous[index] if index < len(previous) else None
        after = current[index] if index < len(current) else None
        name = before.name if before is not None else after.name if after is not None else ""
        if before != after:
            changes.append(
                WindowBoundaryChange(
                    name,
                    before.value if before is not None else None,
                    after.value if after is not None else None,
                )
            )
    return tuple(changes)
