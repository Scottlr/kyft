namespace Kyft;

/// <summary>
/// Describes how recorded windows are normalized before comparison.
/// </summary>
/// <param name="RequireClosedWindows">Whether open windows are rejected during historical comparison.</param>
/// <param name="UseHalfOpenRanges">Whether ranges use half-open start-inclusive, end-exclusive semantics.</param>
public sealed record ComparisonNormalizationPolicy(
    bool RequireClosedWindows,
    bool UseHalfOpenRanges)
{
    /// <summary>
    /// Gets the default historical comparison normalization policy.
    /// </summary>
    public static ComparisonNormalizationPolicy Default { get; } = new(
        RequireClosedWindows: true,
        UseHalfOpenRanges: true);
}
