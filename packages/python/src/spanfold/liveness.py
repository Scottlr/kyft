"""Lane liveness helpers for heartbeat and silence tracking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True, slots=True)
class LaneLivenessSignal:
    """A lane liveness state change emitted by :class:`LaneLivenessTracker`."""

    lane: Any
    is_silent: bool
    occurred_at: datetime
    evaluated_at: datetime
    silence_threshold: timedelta


@dataclass(slots=True)
class _LaneState:
    lane: Any
    started_at: datetime
    last_observed_at: datetime | None = None
    has_reported_state: bool = False
    is_silent: bool = False


class LaneLivenessTracker:
    """Emits deterministic liveness state changes for known lanes.

    The tracker does not own timers, scheduling, persistence, or background
    monitoring. Call :meth:`observe` when a lane reports and :meth:`check` at
    explicit horizons. Returned signals can be ingested into a normal Spanfold
    pipeline to record silence windows.
    """

    def __init__(
        self,
        lanes: list[Any] | tuple[Any, ...],
        *,
        started_at: datetime,
        silence_threshold: timedelta,
    ) -> None:
        if silence_threshold <= timedelta(0):
            msg = "Silence threshold must be greater than zero."
            raise ValueError(msg)
        if not lanes:
            msg = "At least one lane must be tracked."
            raise ValueError(msg)

        self._started_at = started_at
        self._last_check_at = started_at
        self._silence_threshold = silence_threshold
        self._lanes: dict[Any, _LaneState] = {}

        for lane in lanes:
            if lane is None:
                msg = "Tracked lanes cannot include None."
                raise ValueError(msg)
            if lane in self._lanes:
                msg = "Tracked lanes must be unique."
                raise ValueError(msg)
            self._lanes[lane] = _LaneState(lane, started_at)

    @classmethod
    def for_lanes(
        cls,
        started_at: datetime,
        silence_threshold: timedelta,
        *lanes: Any,
    ) -> LaneLivenessTracker:
        """Create a tracker for a fixed set of lanes."""

        return cls(list(lanes), started_at=started_at, silence_threshold=silence_threshold)

    def observe(self, lane: Any, observed_at: datetime) -> tuple[LaneLivenessSignal, ...]:
        """Record that a lane reported at a specific timestamp."""

        state = self._get_lane(lane)
        if observed_at < self._started_at:
            msg = "Observation cannot be earlier than tracker start."
            raise ValueError(msg)
        if state.last_observed_at is not None and observed_at < state.last_observed_at:
            msg = "Observation cannot be earlier than the lane's previous observation."
            raise ValueError(msg)

        state.last_observed_at = observed_at
        if not state.has_reported_state or state.is_silent:
            state.has_reported_state = True
            state.is_silent = False
            return (
                LaneLivenessSignal(
                    lane,
                    False,
                    observed_at,
                    observed_at,
                    self._silence_threshold,
                ),
            )
        return ()

    def check(self, horizon: datetime) -> tuple[LaneLivenessSignal, ...]:
        """Evaluate all tracked lanes at an explicit horizon."""

        if horizon < self._started_at:
            msg = "Liveness horizon cannot be earlier than tracker start."
            raise ValueError(msg)
        if horizon < self._last_check_at:
            msg = "Liveness horizon cannot move backwards."
            raise ValueError(msg)

        self._last_check_at = horizon
        signals: list[LaneLivenessSignal] = []
        for state in self._lanes.values():
            silence_started_at = (
                state.last_observed_at if state.last_observed_at is not None else state.started_at
            ) + self._silence_threshold
            if state.is_silent or horizon < silence_started_at:
                continue
            state.has_reported_state = True
            state.is_silent = True
            signals.append(
                LaneLivenessSignal(
                    state.lane,
                    True,
                    silence_started_at,
                    horizon,
                    self._silence_threshold,
                )
            )
        return tuple(signals)

    def _get_lane(self, lane: Any) -> _LaneState:
        if lane is None:
            msg = "Lane cannot be None."
            raise ValueError(msg)
        try:
            return self._lanes[lane]
        except KeyError as exc:
            msg = "Lane is not tracked by this liveness tracker."
            raise ValueError(msg) from exc
