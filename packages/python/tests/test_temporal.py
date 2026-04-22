from datetime import UTC, datetime

import pytest

from spanfold import TemporalAxis, TemporalPoint, TemporalRange, TemporalRangeEndStatus


def test_position_points_compare_by_position() -> None:
    earlier = TemporalPoint.for_position(10)
    later = TemporalPoint.for_position(20)

    assert earlier.axis is TemporalAxis.PROCESSING_POSITION
    assert earlier.position == 10
    assert earlier < later
    assert later.is_after(earlier)


def test_mixed_axes_are_not_comparable() -> None:
    with pytest.raises(ValueError, match="different axes"):
        assert TemporalPoint.for_position(1) < TemporalPoint.for_timestamp(datetime.now(UTC))


def test_timestamp_points_require_matching_clock() -> None:
    first = TemporalPoint.for_timestamp(datetime(2026, 4, 17, tzinfo=UTC), "event")
    second = TemporalPoint.for_timestamp(datetime(2026, 4, 17, tzinfo=UTC), "received")

    with pytest.raises(ValueError, match="different clock"):
        first.compare_to(second)


def test_half_open_ranges_touching_do_not_overlap() -> None:
    first = TemporalRange.closed(TemporalPoint.for_position(10), TemporalPoint.for_position(20))
    second = TemporalRange.closed(TemporalPoint.for_position(20), TemporalPoint.for_position(30))

    assert not first.overlaps(second)
    assert not second.overlaps(first)
    assert first.contains(TemporalPoint.for_position(10))
    assert not first.contains(TemporalPoint.for_position(20))


def test_open_range_requires_horizon_for_duration() -> None:
    open_range = TemporalRange.open(TemporalPoint.for_position(10))

    assert not open_range.has_end
    with pytest.raises(ValueError, match="without an effective end"):
        open_range.position_length()

    clipped = TemporalRange.with_effective_end(
        TemporalPoint.for_position(10),
        TemporalPoint.for_position(25),
        TemporalRangeEndStatus.OPEN_AT_HORIZON,
    )
    assert clipped.position_length() == 15
    assert not clipped.is_closed


def test_residual_splits_covered_middle() -> None:
    base = TemporalRange.closed(TemporalPoint.for_position(1), TemporalPoint.for_position(10))
    cover = TemporalRange.closed(TemporalPoint.for_position(4), TemporalPoint.for_position(7))

    residual = base.residual([cover])

    assert [(item.start.position, item.require_end().position) for item in residual] == [
        (1, 4),
        (7, 10),
    ]
