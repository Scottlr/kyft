using System.Globalization;

namespace Kyft;

internal static class ComparisonAligner
{
    internal static AlignedComparison Align(PreparedComparison prepared)
    {
        var segments = new List<AlignedSegment>();
        var groups = prepared.NormalizedWindows
            .GroupBy(static window => new AlignmentScope(
                window.Window.WindowName,
                window.Window.Key,
                window.Window.Partition,
                StableObjectValue(window.Window.Key),
                StableObjectValue(window.Window.Partition)))
            .OrderBy(static group => group.Key.WindowName, StringComparer.Ordinal)
            .ThenBy(static group => group.Key.KeySort, StringComparer.Ordinal)
            .ThenBy(static group => group.Key.PartitionSort, StringComparer.Ordinal);

        foreach (var group in groups)
        {
            AddSegments(group.Key, group.ToArray(), segments);
        }

        return new AlignedComparison(prepared, segments.ToArray());
    }

    private static void AddSegments(
        AlignmentScope scope,
        NormalizedWindowRecord[] windows,
        List<AlignedSegment> segments)
    {
        var boundaries = new List<TemporalPoint>(windows.Length * 2);
        for (var i = 0; i < windows.Length; i++)
        {
            var range = windows[i].Range;
            if (!range.HasEnd)
            {
                continue;
            }

            boundaries.Add(range.Start);
            boundaries.Add(range.End!.Value);
        }

        boundaries.Sort(static (left, right) => left.CompareTo(right));

        var unique = new List<TemporalPoint>(boundaries.Count);
        for (var i = 0; i < boundaries.Count; i++)
        {
            if (unique.Count == 0 || boundaries[i].CompareTo(unique[^1]) != 0)
            {
                unique.Add(boundaries[i]);
            }
        }

        for (var i = 0; i < unique.Count - 1; i++)
        {
            var start = unique[i];
            var end = unique[i + 1];
            if (start.CompareTo(end) >= 0)
            {
                continue;
            }

            var targetIds = new List<WindowRecordId>();
            var againstIds = new List<WindowRecordId>();

            for (var windowIndex = 0; windowIndex < windows.Length; windowIndex++)
            {
                var window = windows[windowIndex];
                if (!Covers(window.Range, start, end))
                {
                    continue;
                }

                if (window.Side == ComparisonSide.Target)
                {
                    targetIds.Add(window.RecordId);
                }
                else
                {
                    againstIds.Add(window.RecordId);
                }
            }

            if (targetIds.Count == 0 && againstIds.Count == 0)
            {
                continue;
            }

            segments.Add(new AlignedSegment(
                scope.WindowName,
                scope.Key,
                scope.Partition,
                TemporalRange.Closed(start, end),
                targetIds.ToArray(),
                againstIds.ToArray()));
        }
    }

    private static bool Covers(TemporalRange range, TemporalPoint start, TemporalPoint end)
    {
        return range.HasEnd
            && range.Start.CompareTo(start) <= 0
            && end.CompareTo(range.End!.Value) <= 0;
    }

    private static string StableObjectValue(object? value)
    {
        return value switch
        {
            null => "<null>",
            IFormattable formattable => value.GetType().FullName + ":" + formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.GetType().FullName + ":" + value
        };
    }

    private sealed record AlignmentScope(
        string WindowName,
        object Key,
        object? Partition,
        string KeySort,
        string PartitionSort);
}
