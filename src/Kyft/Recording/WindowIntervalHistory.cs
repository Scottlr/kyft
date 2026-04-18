using Kyft.Internal.Recording;

namespace Kyft;

/// <summary>
/// Stores recorded open and closed windows and exposes window queries.
/// </summary>
public sealed class WindowIntervalHistory
{
    private readonly bool enabled;
    private readonly Dictionary<WindowRecordingKey, OpenWindow> openIntervals;
    private readonly List<ClosedWindow> closedIntervals;

    internal WindowIntervalHistory(bool enabled)
    {
        this.enabled = enabled;
        this.openIntervals = [];
        this.closedIntervals = [];
    }

    /// <summary>
    /// Gets closed windows recorded by the pipeline.
    /// </summary>
    public IReadOnlyList<ClosedWindow> ClosedWindows => this.closedIntervals.ToArray();

    /// <summary>
    /// Gets all recorded windows, including closed windows and currently open windows.
    /// </summary>
    public IReadOnlyList<WindowRecord> Windows
    {
        get
        {
            var windows = new WindowRecord[this.closedIntervals.Count + this.openIntervals.Count];
            var index = 0;

            foreach (var interval in this.closedIntervals)
            {
                windows[index] = interval;
                index++;
            }

            foreach (var interval in this.openIntervals.Values)
            {
                windows[index] = interval;
                index++;
            }

            return windows;
        }
    }

    /// <summary>
    /// Gets currently open windows recorded by the pipeline.
    /// </summary>
    public IReadOnlyList<OpenWindow> OpenWindows
    {
        get
        {
            var intervals = new OpenWindow[this.openIntervals.Count];
            var index = 0;

            foreach (var interval in this.openIntervals.Values)
            {
                intervals[index] = interval;
                index++;
            }

            return intervals;
        }
    }

    /// <summary>
    /// Starts a staged comparison over this recorded window history.
    /// </summary>
    /// <param name="name">A human-readable name for the comparison.</param>
    /// <returns>A comparison builder for the recorded history.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="name" /> is empty or only whitespace.
    /// </exception>
    public WindowComparisonBuilder Compare(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return new WindowComparisonBuilder(this, name);
    }

    /// <summary>
    /// Finds overlapping closed windows within the same window scope.
    /// </summary>
    /// <returns>The overlapping interval pairs.</returns>
    public IReadOnlyList<WindowIntervalOverlap> FindOverlaps()
    {
        var overlaps = new List<WindowIntervalOverlap>();

        for (var i = 0; i < this.closedIntervals.Count; i++)
        {
            var first = this.closedIntervals[i];

            for (var j = i + 1; j < this.closedIntervals.Count; j++)
            {
                var second = this.closedIntervals[j];
                if (!IsSameScope(first, second) || !Overlaps(first, second))
                {
                    continue;
                }

                overlaps.Add(new WindowIntervalOverlap(first, second));
            }
        }

        return overlaps.ToArray();
    }

    /// <summary>
    /// Finds target-source window segments that are not covered by comparison sources.
    /// </summary>
    /// <param name="targetSource">The source whose unique residual segments should be returned.</param>
    /// <returns>The residual segments for the target source.</returns>
    public IReadOnlyList<WindowResidualSegment> FindResiduals(object targetSource)
    {
        ArgumentNullException.ThrowIfNull(targetSource);

        var residuals = new List<WindowResidualSegment>();

        foreach (var target in this.closedIntervals)
        {
            if (!EqualityComparer<object?>.Default.Equals(target.Source, targetSource))
            {
                continue;
            }

            var segments = new List<PositionSegment>
            {
                new(target.StartPosition, ClosedEndPosition(target))
            };

            foreach (var comparison in this.closedIntervals)
            {
                if (ReferenceEquals(target, comparison)
                    || EqualityComparer<object?>.Default.Equals(comparison.Source, targetSource)
                    || !IsSameScope(target, comparison)
                    || !Overlaps(target, comparison))
                {
                    continue;
                }

                Subtract(segments, comparison);
            }

            foreach (var segment in segments)
            {
                if (segment.Start >= segment.End)
                {
                    continue;
                }

                residuals.Add(new WindowResidualSegment(
                    target.WindowName,
                    target.Key,
                    targetSource,
                    segment.Start,
                    segment.End,
                    target.Partition));
            }
        }

        return residuals.ToArray();
    }

    internal void Record<TEvent>(
        IReadOnlyList<WindowEmission<TEvent>> emissions,
        long processingPosition,
        DateTimeOffset? eventTime)
    {
        if (!this.enabled)
        {
            return;
        }

        foreach (var emission in emissions)
        {
            var key = new WindowRecordingKey(
                emission.WindowName,
                emission.Key,
                emission.Source,
                emission.Partition);

            if (emission.Kind == WindowTransitionKind.Opened)
            {
                this.openIntervals[key] = new OpenWindow(
                    emission.WindowName,
                    emission.Key,
                    processingPosition,
                    emission.Source,
                    emission.Partition,
                    eventTime);
                continue;
            }

            if (!this.openIntervals.Remove(key, out var open))
            {
                continue;
            }

            this.closedIntervals.Add(new ClosedWindow(
                open.WindowName,
                open.Key,
                open.StartPosition,
                processingPosition,
                open.Source,
                open.Partition,
                open.StartTime,
                eventTime));
        }
    }

    private static bool IsSameScope(ClosedWindow first, ClosedWindow second)
    {
        return string.Equals(first.WindowName, second.WindowName, StringComparison.Ordinal)
            && EqualityComparer<object>.Default.Equals(first.Key, second.Key)
            && EqualityComparer<object?>.Default.Equals(first.Partition, second.Partition);
    }

    private static bool Overlaps(ClosedWindow first, ClosedWindow second)
    {
        return first.StartPosition < ClosedEndPosition(second)
            && second.StartPosition < ClosedEndPosition(first);
    }

    private static void Subtract(List<PositionSegment> segments, ClosedWindow comparison)
    {
        for (var i = segments.Count - 1; i >= 0; i--)
        {
            var segment = segments[i];
            var overlapStart = Math.Max(segment.Start, comparison.StartPosition);
            var overlapEnd = Math.Min(segment.End, ClosedEndPosition(comparison));

            if (overlapStart >= overlapEnd)
            {
                continue;
            }

            segments.RemoveAt(i);

            if (segment.Start < overlapStart)
            {
                segments.Insert(i, new PositionSegment(segment.Start, overlapStart));
                i++;
            }

            if (overlapEnd < segment.End)
            {
                segments.Insert(i, new PositionSegment(overlapEnd, segment.End));
            }
        }
    }

    private static long ClosedEndPosition(ClosedWindow interval)
    {
        return interval.EndPosition
            ?? throw new InvalidOperationException("Closed windows must have an end position.");
    }

    private readonly record struct PositionSegment(long Start, long End);
}
