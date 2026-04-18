namespace Kyft;

/// <summary>
/// Describes how recorded windows are normalized before comparison.
/// </summary>
/// <param name="RequireClosedWindows">Whether open windows are rejected during historical comparison.</param>
/// <param name="UseHalfOpenRanges">Whether ranges use half-open start-inclusive, end-exclusive semantics.</param>
/// <param name="TimeAxis">The temporal axis used for normalized ranges.</param>
/// <param name="OpenWindowPolicy">How open windows are handled.</param>
/// <param name="OpenWindowHorizon">The optional horizon used when open windows are clipped.</param>
/// <param name="NullTimestampPolicy">How missing event timestamps are handled in event-time mode.</param>
/// <param name="CoalesceAdjacentWindows">Whether adjacent windows with the same scope can be coalesced.</param>
/// <param name="DuplicateWindowPolicy">How duplicate normalized windows are handled.</param>
public sealed record ComparisonNormalizationPolicy(
    bool RequireClosedWindows,
    bool UseHalfOpenRanges,
    TemporalAxis TimeAxis = TemporalAxis.ProcessingPosition,
    ComparisonOpenWindowPolicy OpenWindowPolicy = ComparisonOpenWindowPolicy.RequireClosed,
    TemporalPoint? OpenWindowHorizon = null,
    ComparisonNullTimestampPolicy NullTimestampPolicy = ComparisonNullTimestampPolicy.Reject,
    bool CoalesceAdjacentWindows = false,
    ComparisonDuplicateWindowPolicy DuplicateWindowPolicy = ComparisonDuplicateWindowPolicy.Preserve)
{
    /// <summary>
    /// Gets the default historical comparison normalization policy.
    /// </summary>
    public static ComparisonNormalizationPolicy Default { get; } = new(
        RequireClosedWindows: true,
        UseHalfOpenRanges: true);
}
