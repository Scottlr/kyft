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
        "symmetric-difference",
        "containment"
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
        var containmentRows = new List<ContainmentRow>();
        var leadLagRows = new List<LeadLagRow>();
        var leadLagSummaries = new List<LeadLagSummary>();
        var asOfRows = new List<AsOfRow>();

        for (var i = 0; i < prepared.Plan.Comparators.Count; i++)
        {
            var comparator = prepared.Plan.Comparators[i];
            if (!KnownComparators.Contains(comparator)
                && !TryParseLeadLag(comparator, out _)
                && !TryParseAsOf(comparator, out _))
            {
                diagnostics.Add(new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.UnknownComparator,
                    $"Comparator '{comparator}' is not registered.",
                    $"comparators[{i}]",
                    ComparisonPlanDiagnosticSeverity.Error));
                continue;
            }

            if (TryParseAsOf(comparator, out var asOf))
            {
                var before = asOfRows.Count;
                AddAsOfRows(prepared, asOf, asOfRows, diagnostics);
                summaries.Add(new ComparatorSummary(comparator, asOfRows.Count - before));
                continue;
            }

            if (TryParseLeadLag(comparator, out var leadLag))
            {
                var before = leadLagRows.Count;
                AddLeadLagRows(prepared, leadLag, leadLagRows, leadLagSummaries);
                summaries.Add(new ComparatorSummary(comparator, leadLagRows.Count - before));
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

            if (string.Equals(comparator, "containment", StringComparison.Ordinal))
            {
                var before = containmentRows.Count;
                AddContainmentRows(prepared, aligned, containmentRows);
                summaries.Add(new ComparatorSummary(comparator, containmentRows.Count - before));
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
            symmetricDifferenceRows.ToArray(),
            containmentRows.ToArray(),
            leadLagRows.ToArray(),
            leadLagSummaries.ToArray(),
            asOfRows.ToArray());
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

    private static void AddContainmentRows(
        PreparedComparison prepared,
        AlignedComparison aligned,
        List<ContainmentRow> rows)
    {
        var targetRanges = new Dictionary<WindowRecordId, TemporalRange>();
        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var window = prepared.NormalizedWindows[i];
            if (window.Side == ComparisonSide.Target)
            {
                targetRanges[window.RecordId] = window.Range;
            }
        }

        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            if (segment.TargetRecordIds.Count == 0)
            {
                continue;
            }

            if (segment.AgainstRecordIds.Count > 0)
            {
                rows.Add(new ContainmentRow(
                    segment.WindowName,
                    segment.Key,
                    segment.Partition,
                    segment.Range,
                    ContainmentStatus.Contained,
                    segment.TargetRecordIds,
                    segment.AgainstRecordIds));
                continue;
            }

            for (var targetIndex = 0; targetIndex < segment.TargetRecordIds.Count; targetIndex++)
            {
                var targetId = segment.TargetRecordIds[targetIndex];
                rows.Add(new ContainmentRow(
                    segment.WindowName,
                    segment.Key,
                    segment.Partition,
                    segment.Range,
                    ClassifyUncontainedSegment(targetRanges, targetId, segment.Range),
                    new[] { targetId },
                    Array.Empty<WindowRecordId>()));
            }
        }
    }

    private static ContainmentStatus ClassifyUncontainedSegment(
        Dictionary<WindowRecordId, TemporalRange> targetRanges,
        WindowRecordId targetId,
        TemporalRange segmentRange)
    {
        if (!targetRanges.TryGetValue(targetId, out var targetRange) || !segmentRange.End.HasValue)
        {
            return ContainmentStatus.NotContained;
        }

        if (segmentRange.Start.CompareTo(targetRange.Start) == 0)
        {
            return ContainmentStatus.LeftOverhang;
        }

        if (targetRange.End.HasValue && segmentRange.End.Value.CompareTo(targetRange.End.Value) == 0)
        {
            return ContainmentStatus.RightOverhang;
        }

        return ContainmentStatus.NotContained;
    }

    private static void AddLeadLagRows(
        PreparedComparison prepared,
        LeadLagOptions options,
        List<LeadLagRow> rows,
        List<LeadLagSummary> summaries)
    {
        var before = rows.Count;
        var comparisonTransitions = new Dictionary<TransitionScope, List<TransitionPoint>>();

        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var window = prepared.NormalizedWindows[i];
            if (window.Side != ComparisonSide.Against
                || window.Range.Axis != options.Axis
                || !TryGetTransitionPoint(window.Range, options.Transition, out var point))
            {
                continue;
            }

            var scope = new TransitionScope(window.Window.WindowName, window.Window.Key, window.Window.Partition);
            if (!comparisonTransitions.TryGetValue(scope, out var transitions))
            {
                transitions = [];
                comparisonTransitions.Add(scope, transitions);
            }

            transitions.Add(new TransitionPoint(window.RecordId, point));
        }

        foreach (var pair in comparisonTransitions)
        {
            pair.Value.Sort(static (left, right) => left.Point.CompareTo(right.Point));
        }

        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var target = prepared.NormalizedWindows[i];
            if (target.Side != ComparisonSide.Target
                || target.Range.Axis != options.Axis
                || !TryGetTransitionPoint(target.Range, options.Transition, out var targetPoint))
            {
                continue;
            }

            var scope = new TransitionScope(target.Window.WindowName, target.Window.Key, target.Window.Partition);
            if (!comparisonTransitions.TryGetValue(scope, out var candidates) || candidates.Count == 0)
            {
                rows.Add(new LeadLagRow(
                    target.Window.WindowName,
                    target.Window.Key,
                    target.Window.Partition,
                    options.Transition,
                    options.Axis,
                    targetPoint,
                    ComparisonPoint: null,
                    DeltaMagnitude: null,
                    options.ToleranceMagnitude,
                    IsWithinTolerance: false,
                    LeadLagDirection.MissingComparison,
                    target.RecordId,
                    ComparisonRecordId: null));
                continue;
            }

            var nearest = FindNearest(candidates, targetPoint, options.Axis);
            var delta = GetDeltaMagnitude(targetPoint, nearest.Point, options.Axis);
            var absoluteDelta = Math.Abs(delta);

            rows.Add(new LeadLagRow(
                target.Window.WindowName,
                target.Window.Key,
                target.Window.Partition,
                options.Transition,
                options.Axis,
                targetPoint,
                nearest.Point,
                delta,
                options.ToleranceMagnitude,
                absoluteDelta <= options.ToleranceMagnitude,
                GetDirection(delta),
                target.RecordId,
                nearest.RecordId));
        }

        summaries.Add(CreateLeadLagSummary(options, rows, before));
    }

    private static void AddAsOfRows(
        PreparedComparison prepared,
        AsOfOptions options,
        List<AsOfRow> rows,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        var comparisonTransitions = new Dictionary<TransitionScope, List<TransitionPoint>>();

        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var window = prepared.NormalizedWindows[i];
            if (window.Side != ComparisonSide.Against || window.Range.Axis != options.Axis)
            {
                continue;
            }

            var scope = new TransitionScope(window.Window.WindowName, window.Window.Key, window.Window.Partition);
            if (!comparisonTransitions.TryGetValue(scope, out var transitions))
            {
                transitions = [];
                comparisonTransitions.Add(scope, transitions);
            }

            transitions.Add(new TransitionPoint(window.RecordId, window.Range.Start));
        }

        foreach (var pair in comparisonTransitions)
        {
            pair.Value.Sort(static (left, right) =>
            {
                var pointComparison = left.Point.CompareTo(right.Point);
                return pointComparison != 0
                    ? pointComparison
                    : string.CompareOrdinal(left.RecordId.Value, right.RecordId.Value);
            });
        }

        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var target = prepared.NormalizedWindows[i];
            if (target.Side != ComparisonSide.Target || target.Range.Axis != options.Axis)
            {
                continue;
            }

            var scope = new TransitionScope(target.Window.WindowName, target.Window.Key, target.Window.Partition);
            if (!comparisonTransitions.TryGetValue(scope, out var candidates) || candidates.Count == 0)
            {
                rows.Add(CreateAsOfRow(target, options, target.Range.Start, null, null, AsOfMatchStatus.NoMatch));
                continue;
            }

            var candidate = FindAsOfCandidate(candidates, target.Range.Start, options, out var ambiguous, out var futureRejected);
            if (!candidate.HasValue)
            {
                var future = futureRejected.HasValue
                    ? GetAbsoluteDistance(target.Range.Start, futureRejected.Value.Point, options.Axis)
                    : (long?)null;
                rows.Add(CreateAsOfRow(target, options, target.Range.Start, null, future, futureRejected.HasValue
                    ? AsOfMatchStatus.FutureRejected
                    : AsOfMatchStatus.NoMatch));
                continue;
            }

            var distance = GetAbsoluteDistance(target.Range.Start, candidate.Value.Point, options.Axis);
            if (distance > options.ToleranceMagnitude)
            {
                rows.Add(CreateAsOfRow(target, options, target.Range.Start, null, distance, AsOfMatchStatus.NoMatch));
                continue;
            }

            if (ambiguous)
            {
                diagnostics.Add(new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.AmbiguousAsOfMatch,
                    "As-of lookup found multiple equally eligible comparison transitions; the selected match is deterministic.",
                    $"asof[{target.RecordId}]",
                    ComparisonPlanDiagnosticSeverity.Warning));
            }

            rows.Add(CreateAsOfRow(
                target,
                options,
                target.Range.Start,
                candidate.Value,
                distance,
                ambiguous
                    ? AsOfMatchStatus.Ambiguous
                    : distance == 0
                        ? AsOfMatchStatus.Exact
                        : AsOfMatchStatus.Matched));
        }
    }

    private static AsOfRow CreateAsOfRow(
        NormalizedWindowRecord target,
        AsOfOptions options,
        TemporalPoint targetPoint,
        TransitionPoint? match,
        long? distance,
        AsOfMatchStatus status)
    {
        return new AsOfRow(
            target.Window.WindowName,
            target.Window.Key,
            target.Window.Partition,
            options.Axis,
            options.Direction,
            targetPoint,
            match?.Point,
            distance,
            options.ToleranceMagnitude,
            status,
            target.RecordId,
            match?.RecordId);
    }

    private static TransitionPoint? FindAsOfCandidate(
        List<TransitionPoint> candidates,
        TemporalPoint targetPoint,
        AsOfOptions options,
        out bool ambiguous,
        out TransitionPoint? futureRejected)
    {
        ambiguous = false;
        futureRejected = null;
        TransitionPoint? best = null;
        long? bestDistance = null;

        for (var i = 0; i < candidates.Count; i++)
        {
            var candidate = candidates[i];
            var comparison = candidate.Point.CompareTo(targetPoint);
            if (options.Direction == AsOfDirection.Previous && comparison > 0)
            {
                futureRejected ??= candidate;
                continue;
            }

            if (options.Direction == AsOfDirection.Next && comparison < 0)
            {
                continue;
            }

            var distance = GetAbsoluteDistance(targetPoint, candidate.Point, options.Axis);
            if (!bestDistance.HasValue || distance < bestDistance.Value)
            {
                best = candidate;
                bestDistance = distance;
                ambiguous = false;
                continue;
            }

            if (distance == bestDistance.Value)
            {
                ambiguous = true;
                if (best.HasValue && string.CompareOrdinal(candidate.RecordId.Value, best.Value.RecordId.Value) < 0)
                {
                    best = candidate;
                }
            }
        }

        return best;
    }

    private static bool TryGetTransitionPoint(
        TemporalRange range,
        LeadLagTransition transition,
        out TemporalPoint point)
    {
        if (transition == LeadLagTransition.Start)
        {
            point = range.Start;
            return true;
        }

        if (range.End.HasValue)
        {
            point = range.End.Value;
            return true;
        }

        point = default;
        return false;
    }

    private static TransitionPoint FindNearest(
        List<TransitionPoint> candidates,
        TemporalPoint targetPoint,
        TemporalAxis axis)
    {
        var best = candidates[0];
        var bestDistance = Math.Abs(GetDeltaMagnitude(targetPoint, best.Point, axis));

        for (var i = 1; i < candidates.Count; i++)
        {
            var candidate = candidates[i];
            var distance = Math.Abs(GetDeltaMagnitude(targetPoint, candidate.Point, axis));
            if (distance < bestDistance)
            {
                best = candidate;
                bestDistance = distance;
            }
        }

        return best;
    }

    private static LeadLagSummary CreateLeadLagSummary(
        LeadLagOptions options,
        List<LeadLagRow> rows,
        int startIndex)
    {
        var targetLeadCount = 0;
        var targetLagCount = 0;
        var equalCount = 0;
        var missingCount = 0;
        var outsideToleranceCount = 0;
        long? minimumDelta = null;
        long? maximumDelta = null;

        for (var i = startIndex; i < rows.Count; i++)
        {
            var row = rows[i];
            if (!row.IsWithinTolerance)
            {
                outsideToleranceCount++;
            }

            if (row.Direction == LeadLagDirection.TargetLeads)
            {
                targetLeadCount++;
            }
            else if (row.Direction == LeadLagDirection.TargetLags)
            {
                targetLagCount++;
            }
            else if (row.Direction == LeadLagDirection.Equal)
            {
                equalCount++;
            }
            else if (row.Direction == LeadLagDirection.MissingComparison)
            {
                missingCount++;
            }

            if (row.DeltaMagnitude.HasValue)
            {
                minimumDelta = !minimumDelta.HasValue || row.DeltaMagnitude.Value < minimumDelta.Value
                    ? row.DeltaMagnitude.Value
                    : minimumDelta;
                maximumDelta = !maximumDelta.HasValue || row.DeltaMagnitude.Value > maximumDelta.Value
                    ? row.DeltaMagnitude.Value
                    : maximumDelta;
            }
        }

        return new LeadLagSummary(
            options.Transition,
            options.Axis,
            options.ToleranceMagnitude,
            rows.Count - startIndex,
            targetLeadCount,
            targetLagCount,
            equalCount,
            missingCount,
            outsideToleranceCount,
            minimumDelta,
            maximumDelta);
    }

    private static LeadLagDirection GetDirection(long delta)
    {
        if (delta < 0)
        {
            return LeadLagDirection.TargetLeads;
        }

        return delta > 0
            ? LeadLagDirection.TargetLags
            : LeadLagDirection.Equal;
    }

    private static long GetDeltaMagnitude(
        TemporalPoint targetPoint,
        TemporalPoint comparisonPoint,
        TemporalAxis axis)
    {
        return axis == TemporalAxis.Timestamp
            ? (targetPoint.Timestamp - comparisonPoint.Timestamp).Ticks
            : targetPoint.Position - comparisonPoint.Position;
    }

    private static long GetAbsoluteDistance(
        TemporalPoint targetPoint,
        TemporalPoint comparisonPoint,
        TemporalAxis axis)
    {
        return Math.Abs(GetDeltaMagnitude(targetPoint, comparisonPoint, axis));
    }

    private static bool TryParseLeadLag(string comparator, out LeadLagOptions options)
    {
        options = default;

        var parts = comparator.Split(':');
        if (parts.Length != 4 || !string.Equals(parts[0], "lead-lag", StringComparison.Ordinal))
        {
            return false;
        }

        if (!Enum.TryParse(parts[1], ignoreCase: false, out LeadLagTransition transition)
            || !Enum.TryParse(parts[2], ignoreCase: false, out TemporalAxis axis)
            || axis == TemporalAxis.Unknown
            || !long.TryParse(parts[3], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var toleranceMagnitude)
            || toleranceMagnitude < 0)
        {
            return false;
        }

        options = new LeadLagOptions(transition, axis, toleranceMagnitude);
        return true;
    }

    private static bool TryParseAsOf(string comparator, out AsOfOptions options)
    {
        options = default;

        var parts = comparator.Split(':');
        if (parts.Length != 4 || !string.Equals(parts[0], "asof", StringComparison.Ordinal))
        {
            return false;
        }

        if (!Enum.TryParse(parts[1], ignoreCase: false, out AsOfDirection direction)
            || !Enum.TryParse(parts[2], ignoreCase: false, out TemporalAxis axis)
            || axis == TemporalAxis.Unknown
            || !long.TryParse(parts[3], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var toleranceMagnitude)
            || toleranceMagnitude < 0)
        {
            return false;
        }

        options = new AsOfOptions(direction, axis, toleranceMagnitude);
        return true;
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

    private readonly record struct LeadLagOptions(
        LeadLagTransition Transition,
        TemporalAxis Axis,
        long ToleranceMagnitude);

    private readonly record struct AsOfOptions(
        AsOfDirection Direction,
        TemporalAxis Axis,
        long ToleranceMagnitude);

    private sealed record TransitionScope(string WindowName, object Key, object? Partition);

    private readonly record struct TransitionPoint(WindowRecordId RecordId, TemporalPoint Point);
}
