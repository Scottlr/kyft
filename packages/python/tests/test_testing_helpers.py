import pytest

from spanfold import TemporalPoint, WindowSegment, WindowTag
from spanfold.testing import (
    SpanfoldAssert,
    SpanfoldAssertionError,
    SpanfoldAssertionException,
    SpanfoldSnapshot,
    VirtualComparisonClock,
    WindowHistoryFixtureBuilder,
)


def test_fixture_builder_can_create_comparison_history() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-a")
        .add_closed_window("DeviceOffline", "device-1", 3, 7, source="provider-b")
        .build()
    )

    result = (
        history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("overlap")
        .run()
    )

    SpanfoldAssert.is_valid(result)
    SpanfoldAssert.has_no_diagnostics(result)
    SpanfoldAssert.has_row_count(result, "overlap", 1)


def test_snapshot_helper_normalizes_record_ids() -> None:
    first = "a" * 16
    second = "b" * 64

    SpanfoldSnapshot.assert_equal(
        "ids: <record-id:1> <record-id:2>\n",
        f"ids: {first} {second}",
    )


def test_virtual_clock_produces_deterministic_horizons() -> None:
    clock = VirtualComparisonClock(initial_position=5)

    assert clock.horizon == TemporalPoint.for_position(5)
    assert clock.advance_by(3) == TemporalPoint.for_position(8)
    with pytest.raises(ValueError, match="backwards"):
        clock.advance_to(7)


def test_fixture_builder_can_keep_two_open_segments_for_same_key() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_open_window(
            "DeviceOffline",
            "device-1",
            1,
            source="source-a",
            segments=(WindowSegment("lifecycle", "Normal"),),
        )
        .add_open_window(
            "DeviceOffline",
            "device-1",
            2,
            source="source-a",
            segments=(WindowSegment("lifecycle", "Incident"),),
        )
        .build()
    )

    assert len(history.open_windows) == 2


def test_fixture_builder_can_create_segmented_tagged_windows() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window(
            "DeviceOffline",
            "device-1",
            1,
            5,
            source="source-a",
            segments=(
                WindowSegment("lifecycle", "Incident"),
                WindowSegment("stage", "Escalated", parent_name="lifecycle"),
            ),
            tags=(WindowTag("fleet", "critical"),),
        )
        .build()
    )

    window = history.windows[0]
    assert window.tags[0].value == "critical"
    assert window.segments[1].parent_name == "lifecycle"


def test_assert_helpers_raise_framework_neutral_error() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-a")
        .build()
    )
    result = (
        history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("residual")
        .run()
    )

    with pytest.raises(SpanfoldAssertionError):
        SpanfoldAssert.has_row_count(result, "residual", 2)


def test_assert_helpers_can_find_diagnostics_and_exception_alias() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-a")
        .build()
    )
    result = (
        history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("unknown-comparator")
        .run()
    )

    diagnostic = SpanfoldAssert.has_diagnostic(result, "unknown_comparator")
    assert diagnostic.code == "unknown_comparator"
    assert SpanfoldAssertionException is SpanfoldAssertionError
