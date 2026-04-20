namespace Kyft;

/// <summary>
/// Describes one segment value change that caused a boundary.
/// </summary>
/// <param name="SegmentName">The segment dimension name.</param>
/// <param name="PreviousValue">The previous segment value.</param>
/// <param name="CurrentValue">The current segment value.</param>
public sealed record WindowBoundaryChange(
    string SegmentName,
    object? PreviousValue,
    object? CurrentValue);
