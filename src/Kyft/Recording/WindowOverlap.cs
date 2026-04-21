namespace Kyft;

/// <summary>
/// Describes two closed windows that overlap within the same window scope.
/// </summary>
/// <param name="First">The first overlapping window.</param>
/// <param name="Second">The second overlapping window.</param>
public sealed record WindowOverlap(
    ClosedWindow First,
    ClosedWindow Second);
