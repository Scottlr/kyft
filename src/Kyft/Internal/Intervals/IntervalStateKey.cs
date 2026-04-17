namespace Kyft.Internal.Intervals;

internal sealed record IntervalStateKey(
    string WindowName,
    object Key,
    object? Source,
    object? Partition);
