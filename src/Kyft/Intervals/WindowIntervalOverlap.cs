namespace Kyft;

/// <summary>
/// Describes two closed chunks that overlap within the same window scope.
/// </summary>
/// <param name="First">The first overlapping chunk.</param>
/// <param name="Second">The second overlapping chunk.</param>
public sealed record WindowIntervalOverlap(
    ClosedChunk First,
    ClosedChunk Second);
