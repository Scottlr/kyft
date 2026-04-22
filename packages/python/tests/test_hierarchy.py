from dataclasses import dataclass

from spanfold import ClosedWindow, HierarchyComparisonRowKind, Spanfold, WindowHistory


@dataclass(frozen=True)
class PriceTick:
    selection_id: str
    market_id: str
    fixture_id: str
    price: float


def test_parent_can_be_fully_explained_by_child_windows() -> None:
    history = _manual_hierarchy_history(parent=(1, 5), child=(1, 5))

    result = history.compare_hierarchy("Hierarchy QA", "Parent", "Child")

    assert len(result.rows) == 1
    assert result.rows[0].kind is HierarchyComparisonRowKind.PARENT_EXPLAINED
    assert result.rows[0].range.start.position == 1
    assert result.rows[0].range.require_end().position == 5
    assert result.diagnostics == ()


def test_parent_can_be_partially_unexplained() -> None:
    history = _manual_hierarchy_history(parent=(1, 7), child=(3, 5))

    result = history.compare_hierarchy("Hierarchy QA", "Parent", "Child")

    assert [row.kind for row in result.rows] == [
        HierarchyComparisonRowKind.UNEXPLAINED_PARENT,
        HierarchyComparisonRowKind.PARENT_EXPLAINED,
        HierarchyComparisonRowKind.UNEXPLAINED_PARENT,
    ]


def test_child_outside_parent_is_reported_as_orphan() -> None:
    history = _manual_hierarchy_history(parent=(3, 5), child=(1, 7))

    result = history.compare_hierarchy("Hierarchy QA", "Parent", "Child")

    assert [row.kind for row in result.rows] == [
        HierarchyComparisonRowKind.ORPHAN_CHILD,
        HierarchyComparisonRowKind.PARENT_EXPLAINED,
        HierarchyComparisonRowKind.ORPHAN_CHILD,
    ]


def test_missing_lineage_produces_diagnostic() -> None:
    history = _manual_hierarchy_history(parent=(1, 5), child=None)

    result = history.compare_hierarchy("Hierarchy QA", "Parent", "Child")

    assert result.diagnostics == ("missing_child_lineage",)
    assert result.rows[0].kind is HierarchyComparisonRowKind.UNEXPLAINED_PARENT


def test_rollup_path_is_deterministic_for_hierarchy_comparison() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .window(
            "SelectionSuspension",
            key=lambda tick: tick.selection_id,
            is_active=lambda tick: tick.price == 0,
        )
        .roll_up(
            "MarketSuspension",
            key=lambda tick: tick.market_id,
            is_active=lambda children: children.any_active(),
        )
        .roll_up(
            "FixtureSuspension",
            key=lambda tick: tick.fixture_id,
            is_active=lambda children: children.any_active(),
        )
        .build()
    )

    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 0))
    pipeline.ingest(PriceTick("selection-1", "market-1", "fixture-1", 1.01))

    first = pipeline.history.compare_hierarchy(
        "Market explanation",
        "MarketSuspension",
        "SelectionSuspension",
    )
    second = pipeline.history.compare_hierarchy(
        "Market explanation",
        "MarketSuspension",
        "SelectionSuspension",
    )

    assert first.rows == second.rows
    assert len(first.rows) == 1
    assert first.rows[0].kind is HierarchyComparisonRowKind.PARENT_EXPLAINED


def _manual_hierarchy_history(parent, child):  # type: ignore[no-untyped-def]
    history = WindowHistory(enabled=True)
    if parent is not None:
        history._closed.append(  # noqa: SLF001
            ClosedWindow("Parent", "parent-1", parent[0], parent[1], source="source-a")
        )
    if child is not None:
        history._closed.append(  # noqa: SLF001
            ClosedWindow("Child", "child-1", child[0], child[1], source="source-a")
        )
    return history
