from spanfold import CohortActivity, TemporalPoint, WindowSegment
from spanfold.testing import WindowHistoryFixtureBuilder


def test_residual_against_any_cohort_does_not_double_count_alternating_members() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-b")
        .add_closed_window("SelectionPriced", "selection-1", 6, 11, source="source-c")
        .build()
    )

    result = (
        history.compare("Source A vs cohort")
        .target("source-a")
        .against_cohort("cohort", sources=("source-b", "source-c"))
        .within(window_name="SelectionPriced")
        .using("residual")
        .run()
    )

    assert not result.residual_rows


def test_residual_against_all_cohort_requires_every_member_active() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-b")
        .add_closed_window("SelectionPriced", "selection-1", 6, 11, source="source-c")
        .build()
    )

    result = (
        history.compare("Source A vs full cohort")
        .target("source-a")
        .against_cohort(
            "cohort",
            sources=("source-b", "source-c"),
            activity=CohortActivity.all(),
        )
        .within(window_name="SelectionPriced")
        .using("residual")
        .run()
    )

    assert sum(row.range.position_length() for row in result.residual_rows) == 10


def test_threshold_cohort_uses_required_active_count() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-b")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-c")
        .add_closed_window("SelectionPriced", "selection-1", 6, 11, source="source-d")
        .build()
    )

    result = (
        history.compare("Source A vs threshold cohort")
        .target("source-a")
        .against_cohort(
            "cohort",
            sources=("source-b", "source-c", "source-d"),
            activity=CohortActivity.at_least(2),
        )
        .within(window_name="SelectionPriced")
        .using("residual")
        .run()
    )

    assert not result.residual_rows


def test_none_cohort_requires_no_active_members() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-b")
        .build()
    )

    result = (
        history.compare("Source A vs none cohort")
        .target("source-a")
        .against_cohort(
            "cohort",
            sources=("source-b", "source-c"),
            activity=CohortActivity.none(),
        )
        .within(window_name="SelectionPriced")
        .using("residual")
        .run()
    )

    assert sum(row.range.position_length() for row in result.residual_rows) == 5


def test_segment_filters_apply_before_cohort_materialization() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window(
            "SelectionPriced",
            "selection-1",
            1,
            11,
            source="source-a",
            segments=(WindowSegment("tradingState", "Suspended"),),
        )
        .add_closed_window(
            "SelectionPriced",
            "selection-1",
            1,
            6,
            source="source-b",
            segments=(WindowSegment("tradingState", "Suspended"),),
        )
        .add_closed_window(
            "SelectionPriced",
            "selection-1",
            6,
            11,
            source="source-c",
            segments=(WindowSegment("tradingState", "Open"),),
        )
        .build()
    )

    result = (
        history.compare("Source A suspended vs cohort")
        .target("source-a")
        .against_cohort("cohort", sources=("source-b", "source-c"))
        .within(
            window_name="SelectionPriced",
            segments={"tradingState": "Suspended"},
        )
        .using("residual")
        .run()
    )

    assert sum(row.range.position_length() for row in result.residual_rows) == 5


def test_known_at_filtering_runs_before_cohort_materialization() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-b")
        .add_closed_window("SelectionPriced", "selection-1", 6, 11, source="source-c")
        .build()
    )

    result = (
        history.compare("Known-at cohort")
        .target("source-a")
        .against_cohort("cohort", sources=("source-b", "source-c"))
        .within(window_name="SelectionPriced")
        .normalize(known_at=TemporalPoint.for_position(5))
        .using("residual")
        .run()
    )

    assert not result.residual_rows
    assert "future_window_excluded" in result.diagnostics


def test_known_at_excludes_future_segment_windows() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window(
            "SelectionPriced",
            "selection-1",
            1,
            5,
            source="source-a",
            segments=(WindowSegment("period", "FirstPeriod"),),
        )
        .add_closed_window(
            "SelectionPriced",
            "selection-1",
            6,
            10,
            source="source-a",
            segments=(WindowSegment("period", "SecondPeriod"),),
        )
        .build()
    )

    result = (
        history.compare("Known-at segmented audit")
        .target("source-a")
        .against("source-b")
        .within(window_name="SelectionPriced")
        .normalize(known_at=TemporalPoint.for_position(5))
        .using("residual")
        .run()
    )

    assert [
        (row.range.start.position, row.range.require_end().position)
        for row in result.residual_rows
    ] == [(1, 5)]
    assert "future_window_excluded" in result.diagnostics


def test_known_at_cohort_activity_does_not_use_unavailable_members() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-a")
        .add_closed_window("SelectionPriced", "selection-1", 1, 6, source="source-b")
        .add_closed_window("SelectionPriced", "selection-1", 1, 11, source="source-c")
        .build()
    )

    result = (
        history.compare("Known-at cohort threshold")
        .target("source-a")
        .against_cohort(
            "cohort",
            sources=("source-b", "source-c"),
            activity=CohortActivity.at_least(2),
        )
        .within(window_name="SelectionPriced")
        .normalize(known_at=TemporalPoint.for_position(6))
        .using("residual")
        .run()
    )

    assert sum(row.range.position_length() for row in result.residual_rows) == 5
    assert "future_window_excluded" in result.diagnostics
