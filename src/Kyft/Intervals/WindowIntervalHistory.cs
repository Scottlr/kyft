using Kyft.Internal.Intervals;

namespace Kyft;

/// <summary>
/// Stores recorded open and closed chunks and exposes chunk queries.
/// </summary>
public sealed class WindowIntervalHistory
{
    private readonly bool enabled;
    private readonly Dictionary<IntervalStateKey, OpenChunk> openIntervals;
    private readonly List<ClosedChunk> closedIntervals;

    internal WindowIntervalHistory(bool enabled)
    {
        this.enabled = enabled;
        this.openIntervals = [];
        this.closedIntervals = [];
    }

    /// <summary>
    /// Gets closed chunks recorded by the pipeline.
    /// </summary>
    public IReadOnlyList<ClosedChunk> ClosedChunks => this.closedIntervals.ToArray();

    /// <summary>
    /// Gets all recorded chunks, including closed chunks and currently open chunks.
    /// </summary>
    public IReadOnlyList<Chunk> Chunks
    {
        get
        {
            var chunks = new Chunk[this.closedIntervals.Count + this.openIntervals.Count];
            var index = 0;

            foreach (var interval in this.closedIntervals)
            {
                chunks[index] = interval;
                index++;
            }

            foreach (var interval in this.openIntervals.Values)
            {
                chunks[index] = interval;
                index++;
            }

            return chunks;
        }
    }

    /// <summary>
    /// Gets currently open chunks recorded by the pipeline.
    /// </summary>
    public IReadOnlyList<OpenChunk> OpenChunks
    {
        get
        {
            var intervals = new OpenChunk[this.openIntervals.Count];
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
    /// Finds overlapping closed chunks within the same window scope.
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
    /// Finds target-source chunk segments that are not covered by comparison sources.
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
            var key = new IntervalStateKey(
                emission.WindowName,
                emission.Key,
                emission.Source,
                emission.Partition);

            if (emission.Kind == WindowTransitionKind.Opened)
            {
                this.openIntervals[key] = new OpenChunk(
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

            this.closedIntervals.Add(new ClosedChunk(
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

    private static bool IsSameScope(ClosedChunk first, ClosedChunk second)
    {
        return string.Equals(first.WindowName, second.WindowName, StringComparison.Ordinal)
            && EqualityComparer<object>.Default.Equals(first.Key, second.Key)
            && EqualityComparer<object?>.Default.Equals(first.Partition, second.Partition);
    }

    private static bool Overlaps(ClosedChunk first, ClosedChunk second)
    {
        return first.StartPosition < ClosedEndPosition(second)
            && second.StartPosition < ClosedEndPosition(first);
    }

    private static void Subtract(List<PositionSegment> segments, ClosedChunk comparison)
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

    private static long ClosedEndPosition(ClosedChunk interval)
    {
        return interval.EndPosition
            ?? throw new InvalidOperationException("Closed chunks must have an end position.");
    }

    private readonly record struct PositionSegment(long Start, long End);
}
