namespace Kyft;

/// <summary>
/// Describes one parent/child temporal contribution segment.
/// </summary>
/// <param name="Kind">The hierarchy explanation kind.</param>
/// <param name="Source">The optional source identity shared by the row.</param>
/// <param name="Partition">The optional partition identity shared by the row.</param>
/// <param name="Range">The temporal range of the row.</param>
/// <param name="ParentRecordIds">The active parent record IDs.</param>
/// <param name="ChildRecordIds">The active child contribution record IDs.</param>
public sealed record HierarchyComparisonRow(
    HierarchyComparisonRowKind Kind,
    object? Source,
    object? Partition,
    TemporalRange Range,
    IReadOnlyList<WindowRecordId> ParentRecordIds,
    IReadOnlyList<WindowRecordId> ChildRecordIds);
