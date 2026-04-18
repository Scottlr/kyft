using Kyft;

namespace Kyft.Internal.Comparison;

internal static class ComparisonRuntime
{
    private static readonly HashSet<string> KnownComparators = new(StringComparer.Ordinal)
    {
        "overlap",
        "residual",
        "missing",
        "coverage",
        "gap",
        "symmetric-difference"
    };

    internal static ComparisonResult Run(PreparedComparison prepared)
    {
        var aligned = prepared.Align();
        var diagnostics = new List<ComparisonPlanDiagnostic>(prepared.Diagnostics);
        var summaries = new List<ComparatorSummary>();
        var overlapRows = new List<OverlapRow>();
        var residualRows = new List<ResidualRow>();
        var missingRows = new List<MissingRow>();
        var coverageRows = new List<CoverageRow>();
        var coverageSummaries = new List<CoverageSummary>();
        var gapRows = new List<GapRow>();
        var symmetricDifferenceRows = new List<SymmetricDifferenceRow>();

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

            if (string.Equals(comparator, "residual", StringComparison.Ordinal))
            {
                var before = residualRows.Count;
                AddResidualRows(aligned, residualRows);
                summaries.Add(new ComparatorSummary(comparator, residualRows.Count - before));
                continue;
            }

            if (string.Equals(comparator, "missing", StringComparison.Ordinal))
            {
                var before = missingRows.Count;
                AddMissingRows(aligned, missingRows);
                summaries.Add(new ComparatorSummary(comparator, missingRows.Count - before));
                continue;
            }

            if (string.Equals(comparator, "coverage", StringComparison.Ordinal))
            {
                var before = coverageRows.Count;
                AddCoverageRows(aligned, coverageRows, coverageSummaries);
                summaries.Add(new ComparatorSummary(comparator, coverageRows.Count - before));
                continue;
            }

            if (string.Equals(comparator, "gap", StringComparison.Ordinal))
            {
                var before = gapRows.Count;
                AddGapRows(aligned, gapRows);
                summaries.Add(new ComparatorSummary(comparator, gapRows.Count - before));
                continue;
            }

            if (string.Equals(comparator, "symmetric-difference", StringComparison.Ordinal))
            {
                var before = symmetricDifferenceRows.Count;
                AddSymmetricDifferenceRows(aligned, symmetricDifferenceRows);
                summaries.Add(new ComparatorSummary(comparator, symmetricDifferenceRows.Count - before));
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
            overlapRows.ToArray(),
            residualRows.ToArray(),
            missingRows.ToArray(),
            coverageRows.ToArray(),
            coverageSummaries.ToArray(),
            gapRows.ToArray(),
            symmetricDifferenceRows.ToArray());
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

    private static void AddResidualRows(AlignedComparison aligned, List<ResidualRow> rows)
    {
        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            if (segment.TargetRecordIds.Count == 0 || segment.AgainstRecordIds.Count != 0)
            {
                continue;
            }

            rows.Add(new ResidualRow(
                segment.WindowName,
                segment.Key,
                segment.Partition,
                segment.Range,
                segment.TargetRecordIds));
        }
    }

    private static void AddMissingRows(AlignedComparison aligned, List<MissingRow> rows)
    {
        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            if (segment.TargetRecordIds.Count != 0 || segment.AgainstRecordIds.Count == 0)
            {
                continue;
            }

            rows.Add(new MissingRow(
                segment.WindowName,
                segment.Key,
                segment.Partition,
                segment.Range,
                segment.AgainstRecordIds));
        }
    }

    private static void AddCoverageRows(
        AlignedComparison aligned,
        List<CoverageRow> rows,
        List<CoverageSummary> summaries)
    {
        var summary = new Dictionary<CoverageScope, (double Target, double Covered)>();

        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            if (segment.TargetRecordIds.Count == 0)
            {
                continue;
            }

            var targetMagnitude = Measure(segment.Range);
            var coveredMagnitude = segment.AgainstRecordIds.Count > 0 ? targetMagnitude : 0d;

            rows.Add(new CoverageRow(
                segment.WindowName,
                segment.Key,
                segment.Partition,
                segment.Range,
                targetMagnitude,
                coveredMagnitude,
                segment.TargetRecordIds,
                segment.AgainstRecordIds));

            var key = new CoverageScope(segment.WindowName, segment.Key, segment.Partition);
            summary.TryGetValue(key, out var totals);
            summary[key] = (totals.Target + targetMagnitude, totals.Covered + coveredMagnitude);
        }

        foreach (var item in summary.OrderBy(static pair => pair.Key.WindowName, StringComparer.Ordinal))
        {
            summaries.Add(new CoverageSummary(
                item.Key.WindowName,
                item.Key.Key,
                item.Key.Partition,
                item.Value.Target,
                item.Value.Covered,
                item.Value.Target == 0d ? 0d : item.Value.Covered / item.Value.Target));
        }
    }

    private static void AddGapRows(AlignedComparison aligned, List<GapRow> rows)
    {
        for (var i = 0; i < aligned.Segments.Count - 1; i++)
        {
            var current = aligned.Segments[i];
            var next = aligned.Segments[i + 1];

            if (!IsSameScope(current, next) || !current.Range.End.HasValue)
            {
                continue;
            }

            var gapStart = current.Range.End.Value;
            var gapEnd = next.Range.Start;
            if (gapStart.CompareTo(gapEnd) >= 0)
            {
                continue;
            }

            rows.Add(new GapRow(
                current.WindowName,
                current.Key,
                current.Partition,
                TemporalRange.Closed(gapStart, gapEnd)));
        }
    }

    private static void AddSymmetricDifferenceRows(
        AlignedComparison aligned,
        List<SymmetricDifferenceRow> rows)
    {
        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            var hasTarget = segment.TargetRecordIds.Count > 0;
            var hasAgainst = segment.AgainstRecordIds.Count > 0;

            if (hasTarget == hasAgainst)
            {
                continue;
            }

            rows.Add(new SymmetricDifferenceRow(
                segment.WindowName,
                segment.Key,
                segment.Partition,
                segment.Range,
                hasTarget ? ComparisonSide.Target : ComparisonSide.Against,
                segment.TargetRecordIds,
                segment.AgainstRecordIds));
        }
    }

    private static double Measure(TemporalRange range)
    {
        return range.Axis == TemporalAxis.Timestamp
            ? range.GetTimeDuration().Ticks
            : range.GetPositionLength();
    }

    private static bool IsSameScope(AlignedSegment first, AlignedSegment second)
    {
        return string.Equals(first.WindowName, second.WindowName, StringComparison.Ordinal)
            && EqualityComparer<object>.Default.Equals(first.Key, second.Key)
            && EqualityComparer<object?>.Default.Equals(first.Partition, second.Partition);
    }

    private sealed record CoverageScope(string WindowName, object Key, object? Partition);
}
