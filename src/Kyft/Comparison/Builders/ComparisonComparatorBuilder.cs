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

    internal IReadOnlyList<string> Build()
    {
        return this.comparators.ToArray();
    }
}
