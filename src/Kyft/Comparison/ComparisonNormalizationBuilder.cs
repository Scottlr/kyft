namespace Kyft;

/// <summary>
/// Builds normalization policy for a comparison plan.
/// </summary>
public sealed class ComparisonNormalizationBuilder
{
    private bool requireClosedWindows = true;
    private bool useHalfOpenRanges = true;

    /// <summary>
    /// Requires recorded windows to be closed before historical comparison.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonNormalizationBuilder RequireClosedWindows()
    {
        this.requireClosedWindows = true;
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

    internal ComparisonNormalizationPolicy Build()
    {
        return new ComparisonNormalizationPolicy(this.requireClosedWindows, this.useHalfOpenRanges);
    }
}
