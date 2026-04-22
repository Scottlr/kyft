from dataclasses import dataclass

import pytest

from spanfold import Spanfold, WindowSegment, WindowTransitionKind


@dataclass(frozen=True)
class PriceTick:
    selection_id: str
    market_id: str
    fixture_id: str
    price: float
    phase: str = "Pregame"
    state: str = "Open"


def test_rollup_opens_when_all_known_children_are_active() -> None:
    pipeline = _market_pipeline()

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-2", "market-1", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 0))
    result = pipeline.ingest(PriceTick("selection-2", "market-1", "fixture-1", 0))

    assert [emission.window_name for emission in result.emissions] == [
        "SelectionSuspension",
        "MarketSuspension",
    ]
    assert result.emissions[1].key == "market-1"
    assert result.emissions[1].kind is WindowTransitionKind.OPENED


def test_rollup_closes_when_any_known_child_becomes_inactive() -> None:
    pipeline = _market_pipeline()

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-2", "market-1", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 0))
    pipeline.ingest(PriceTick("selection-2", "market-1", "fixture-1", 0))
    result = pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))

    assert [emission.window_name for emission in result.emissions] == [
        "SelectionSuspension",
        "MarketSuspension",
    ]
    assert result.emissions[1].kind is WindowTransitionKind.CLOSED


def test_rollup_can_feed_another_rollup() -> None:
    pipeline = (
        Spanfold.for_events()
        .window(
            "SelectionSuspension",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price == 0,
        )
        .roll_up(
            "MarketSuspension",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.all_active(),
        )
        .roll_up(
            "FixtureSuspension",
            key=lambda tick: tick.fixture_id,
            is_active=lambda children: children.all_active(),
        )
        .build()
    )

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-2", "market-2", "fixture-1", 1.01))
    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 0))
    result = pipeline.ingest(PriceTick("selection-2", "market-2", "fixture-1", 0))

    assert [emission.window_name for emission in result.emissions] == [
        "SelectionSuspension",
        "MarketSuspension",
        "FixtureSuspension",
    ]
    assert result.emissions[2].key == "fixture-1"


def test_rollups_preserve_child_segment_context_and_reopen_on_segment_change() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .window(
            "SelectionPriced",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price > 0,
            segments=lambda tick: {"phase": tick.phase},
        )
        .roll_up(
            "MarketPriced",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.any_active(),
        )
        .build()
    )

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01, "Pregame"))
    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01, "InPlay"))

    closed = [
        window
        for window in pipeline.history.closed_windows
        if window.window_name == "MarketPriced"
    ]
    open_ = [
        window for window in pipeline.history.open_windows if window.window_name == "MarketPriced"
    ]

    assert closed[0].segments[0].value == "Pregame"
    assert open_[0].segments[0].value == "InPlay"


def test_rollup_segment_projection_can_drop_rename_and_transform() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .window(
            "SelectionPriced",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price > 0,
            segments=lambda tick: (
                WindowSegment("phase", tick.phase),
                WindowSegment("state", tick.state, parent_name="phase"),
            ),
        )
        .roll_up(
            "MarketPriced",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.any_active(),
            preserve_segments=("phase",),
            rename_segments={"phase": "lifecycle"},
            transform_segments={"phase": lambda value: str(value).upper()},
        )
        .build()
    )

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01, "in-play"))

    market = [
        window for window in pipeline.history.open_windows if window.window_name == "MarketPriced"
    ][0]
    assert market.segments == (WindowSegment("lifecycle", "IN-PLAY"),)


def test_rollup_rejects_duplicate_projected_segment_names() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .window(
            "SelectionPriced",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price > 0,
            segments=lambda tick: (
                WindowSegment("phase", tick.phase),
                WindowSegment("state", tick.state),
            ),
        )
        .roll_up(
            "MarketPriced",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.any_active(),
            rename_segments={"state": "phase"},
        )
        .build()
    )

    with pytest.raises(ValueError, match="duplicate segment 'phase'"):
        pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))


def _market_pipeline():
    return (
        Spanfold.for_events()
        .window(
            "SelectionSuspension",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price == 0,
        )
        .roll_up(
            "MarketSuspension",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.all_active(),
        )
        .build()
    )
