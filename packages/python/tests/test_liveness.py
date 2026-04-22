from datetime import UTC, datetime, timedelta

import pytest

from spanfold import LaneLivenessTracker, Spanfold


def test_first_observation_emits_alive_once() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")

    first = tracker.observe("lane-a", started_at + timedelta(seconds=5))
    second = tracker.observe("lane-a", started_at + timedelta(seconds=10))

    assert len(first) == 1
    assert first[0].lane == "lane-a"
    assert not first[0].is_silent
    assert first[0].occurred_at == started_at + timedelta(seconds=5)
    assert second == ()


def test_check_emits_silence_once_when_lane_expires() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")
    tracker.observe("lane-a", started_at + timedelta(seconds=5))

    early = tracker.check(started_at + timedelta(seconds=34))
    expired = tracker.check(started_at + timedelta(seconds=40))
    repeated = tracker.check(started_at + timedelta(seconds=50))

    assert early == ()
    assert len(expired) == 1
    assert expired[0].is_silent
    assert expired[0].occurred_at == started_at + timedelta(seconds=35)
    assert expired[0].evaluated_at == started_at + timedelta(seconds=40)
    assert repeated == ()


def test_observation_after_silence_emits_recovery() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")

    tracker.observe("lane-a", started_at)
    tracker.check(started_at + timedelta(seconds=31))
    recovery = tracker.observe("lane-a", started_at + timedelta(seconds=45))

    assert len(recovery) == 1
    assert not recovery[0].is_silent
    assert recovery[0].occurred_at == started_at + timedelta(seconds=45)


def test_check_can_emit_silence_for_lane_that_never_reported() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")

    signal = tracker.check(started_at + timedelta(seconds=40))[0]

    assert signal.is_silent
    assert signal.occurred_at == started_at + timedelta(seconds=30)
    assert signal.evaluated_at == started_at + timedelta(seconds=40)


def test_liveness_signals_can_record_silence_windows() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .with_event_time(lambda signal: signal.occurred_at)
        .track_window(
            "LaneSilent",
            key=lambda signal: signal.lane,
            is_active=lambda signal: signal.is_silent,
            tags=lambda signal: {"threshold": signal.silence_threshold},
        )
    )

    for signal in tracker.observe("lane-a", started_at):
        pipeline.ingest(signal, source="liveness")
    for signal in tracker.check(started_at + timedelta(seconds=31)):
        pipeline.ingest(signal, source="liveness")
    for signal in tracker.observe("lane-a", started_at + timedelta(seconds=45)):
        pipeline.ingest(signal, source="liveness")

    window = (
        pipeline.history.query()
        .where_window("LaneSilent")
        .where_key("lane-a")
        .closed()
        .all()[0]
    )

    assert window.start_time == started_at + timedelta(seconds=30)
    assert window.end_time == started_at + timedelta(seconds=45)
    assert window.source == "liveness"


def test_tracker_rejects_unknown_lane() -> None:
    started_at = datetime(2026, 4, 21, 10, tzinfo=UTC)
    tracker = LaneLivenessTracker.for_lanes(started_at, timedelta(seconds=30), "lane-a")

    with pytest.raises(ValueError, match="not tracked"):
        tracker.observe("lane-b", started_at)
