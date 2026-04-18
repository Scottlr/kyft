namespace Kyft;

internal static class ComparisonRuntime
{
    private static readonly HashSet<string> KnownComparators = new(StringComparer.Ordinal)
    {
        "overlap",
        "residual",
        "missing",
        "coverage"
    };

    internal static ComparisonResult Run(PreparedComparison prepared)
    {
        var aligned = prepared.Align();
        var diagnostics = new List<ComparisonPlanDiagnostic>(prepared.Diagnostics);
        var summaries = new List<ComparatorSummary>();
        var overlapRows = new List<OverlapRow>();

        for (var i = 0; i < prepared.Plan.Comparators.Count; i++)
        {
            var comparator = prepared.Plan.Comparators[i];
            if (!KnownComparators.Contains(comparator))
            {
                diagnostics.Add(new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.UnknownComparator,
                    $"Comparator '{comparator}' is not registered.",
                    $"comparators[{i}]",
                    ComparisonPlanDiagnosticSeverity.Error));
                continue;
            }

            if (string.Equals(comparator, "overlap", StringComparison.Ordinal))
            {
                var before = overlapRows.Count;
                AddOverlapRows(aligned, overlapRows);
                summaries.Add(new ComparatorSummary(comparator, overlapRows.Count - before));
                continue;
            }

            summaries.Add(new ComparatorSummary(comparator, RowCount: 0));
        }

        return new ComparisonResult(
            prepared.Plan,
            diagnostics.ToArray(),
            prepared,
            aligned,
            summaries.ToArray(),
            overlapRows.ToArray());
    }

    private static void AddOverlapRows(AlignedComparison aligned, List<OverlapRow> rows)
    {
        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            if (segment.TargetRecordIds.Count == 0 || segment.AgainstRecordIds.Count == 0)
            {
                continue;
            }

            rows.Add(new OverlapRow(
                segment.WindowName,
                segment.Key,
                segment.Partition,
                segment.Range,
                segment.TargetRecordIds,
                segment.AgainstRecordIds));
        }
    }
}
