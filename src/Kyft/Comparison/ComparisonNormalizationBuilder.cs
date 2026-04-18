namespace Kyft;

/// <summary>
/// Builds normalization policy for a comparison plan.
/// </summary>
public sealed class ComparisonNormalizationBuilder
{
    private bool requireClosedWindows = true;
    private bool useHalfOpenRanges = true;
    private TemporalAxis timeAxis = TemporalAxis.ProcessingPosition;
    private ComparisonOpenWindowPolicy openWindowPolicy = ComparisonOpenWindowPolicy.RequireClosed;
    private TemporalPoint? openWindowHorizon;
    private ComparisonNullTimestampPolicy nullTimestampPolicy = ComparisonNullTimestampPolicy.Reject;
    private bool coalesceAdjacentWindows;
    private ComparisonDuplicateWindowPolicy duplicateWindowPolicy = ComparisonDuplicateWindowPolicy.Preserve;

    /// <summary>
    /// Requires recorded windows to be closed before historical comparison.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder RequireClosedWindows()
    {
        this.requireClosedWindows = true;
        this.openWindowPolicy = ComparisonOpenWindowPolicy.RequireClosed;
        this.openWindowHorizon = null;
        return this;
    }

    /// <summary>
    /// Clips open windows to an explicit horizon.
    /// </summary>
    /// <param name="horizon">The effective end for open windows.</param>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder ClipOpenWindowsTo(TemporalPoint horizon)
    {
        this.requireClosedWindows = false;
        this.openWindowPolicy = ComparisonOpenWindowPolicy.ClipToHorizon;
        this.openWindowHorizon = horizon;
        return this;
    }

    /// <summary>
    /// Uses half-open start-inclusive, end-exclusive ranges.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder HalfOpen()
    {
        this.useHalfOpenRanges = true;
        return this;
    }

    /// <summary>
    /// Normalizes windows on the processing-position axis.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder OnPosition()
    {
        this.timeAxis = TemporalAxis.ProcessingPosition;
        return this;
    }

    /// <summary>
    /// Normalizes windows on the event-time axis.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder OnEventTime()
    {
        this.timeAxis = TemporalAxis.Timestamp;
        return this;
    }

    /// <summary>
    /// Rejects records with missing event timestamps in event-time mode.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder RejectMissingEventTime()
    {
        this.nullTimestampPolicy = ComparisonNullTimestampPolicy.Reject;
        return this;
    }

    /// <summary>
    /// Excludes records with missing event timestamps in event-time mode.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder ExcludeMissingEventTime()
    {
        this.nullTimestampPolicy = ComparisonNullTimestampPolicy.Exclude;
        return this;
    }

    /// <summary>
    /// Coalesces adjacent normalized windows with identical comparison scope.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder CoalesceAdjacentWindows()
    {
        this.coalesceAdjacentWindows = true;
        return this;
    }

    /// <summary>
    /// Rejects duplicate normalized windows.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder RejectDuplicateWindows()
    {
        this.duplicateWindowPolicy = ComparisonDuplicateWindowPolicy.Reject;
        return this;
    }

    internal ComparisonNormalizationPolicy Build()
    {
        return new ComparisonNormalizationPolicy(
            this.requireClosedWindows,
            this.useHalfOpenRanges,
            this.timeAxis,
            this.openWindowPolicy,
            this.openWindowHorizon,
            this.nullTimestampPolicy,
            this.coalesceAdjacentWindows,
            this.duplicateWindowPolicy);
    }
}
