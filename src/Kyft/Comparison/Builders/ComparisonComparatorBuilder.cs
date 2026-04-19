namespace Kyft;

/// <summary>
/// Builds comparator declarations for a comparison plan.
/// </summary>
public sealed class ComparisonComparatorBuilder
{
    private readonly List<string> comparators = [];

    /// <summary>
    /// Adds the overlap comparator.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Overlap()
    {
        this.comparators.Add("overlap");
        return this;
    }

    /// <summary>
    /// Adds the residual comparator.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Residual()
    {
        this.comparators.Add("residual");
        return this;
    }

    /// <summary>
    /// Adds the missing comparator.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Missing()
    {
        this.comparators.Add("missing");
        return this;
    }

    /// <summary>
    /// Adds the coverage comparator.
    /// </summary>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Coverage()
    {
        this.comparators.Add("coverage");
        return this;
    }

    /// <summary>
    /// Adds the gap comparator.
    /// </summary>
    /// <remarks>
    /// Gap rows report spaces where neither side is active inside an observed
    /// comparison scope envelope.
    /// </remarks>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Gap()
    {
        this.comparators.Add("gap");
        return this;
    }

    /// <summary>
    /// Adds the symmetric-difference comparator.
    /// </summary>
    /// <remarks>
    /// Symmetric-difference rows report both target-only and comparison-only
    /// disagreement segments.
    /// </remarks>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder SymmetricDifference()
    {
        this.comparators.Add("symmetric-difference");
        return this;
    }

    /// <summary>
    /// Adds the containment comparator.
    /// </summary>
    /// <remarks>
    /// Containment is directional: target windows are checked for coverage by
    /// comparison windows.
    /// </remarks>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder Containment()
    {
        this.comparators.Add("containment");
        return this;
    }

    /// <summary>
    /// Adds the lead/lag comparator with an explicit transition, axis, and tolerance.
    /// </summary>
    /// <remarks>
    /// For processing-position comparisons, <paramref name="toleranceMagnitude" />
    /// is measured in positions. For timestamp comparisons, it is measured in
    /// ticks. Lead/lag is transition-based and does not imply causality.
    /// </remarks>
    /// <param name="transition">The transition point to compare.</param>
    /// <param name="axis">The temporal axis to measure.</param>
    /// <param name="toleranceMagnitude">The allowed absolute delta.</param>
    /// <returns>This builder.</returns>
    public ComparisonComparatorBuilder LeadLag(
        LeadLagTransition transition,
        TemporalAxis axis,
        long toleranceMagnitude)
    {
        if (axis == TemporalAxis.Unknown)
        {
            throw new ArgumentException("Lead/lag requires an explicit temporal axis.", nameof(axis));
        }

        if (toleranceMagnitude < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(toleranceMagnitude), "Lead/lag tolerance cannot be negative.");
        }

        this.comparators.Add($"lead-lag:{transition}:{axis}:{toleranceMagnitude}");
        return this;
    }

    internal IReadOnlyList<string> Build()
    {
        return this.comparators.ToArray();
    }
}
