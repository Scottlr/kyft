namespace Kyft;

/// <summary>
/// Builds a comparison cohort declaration.
/// </summary>
public sealed class ComparisonCohortBuilder
{
    private readonly List<object> sources;
    private CohortActivity activity;

    /// <summary>
    /// Creates a cohort builder.
    /// </summary>
    public ComparisonCohortBuilder()
    {
        this.sources = [];
        this.activity = CohortActivity.Any();
    }

    /// <summary>
    /// Adds source identities to the cohort.
    /// </summary>
    /// <param name="sources">The source identities.</param>
    /// <returns>The current builder.</returns>
    public ComparisonCohortBuilder Sources(params object[] sources)
    {
        ArgumentNullException.ThrowIfNull(sources);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentNullException.ThrowIfNull(sources[i]);
            this.sources.Add(sources[i]);
        }

        return this;
    }

    /// <summary>
    /// Sets the cohort activity rule.
    /// </summary>
    /// <param name="activity">The cohort activity rule.</param>
    /// <returns>The current builder.</returns>
    public ComparisonCohortBuilder Activity(CohortActivity activity)
    {
        ArgumentNullException.ThrowIfNull(activity);

        this.activity = activity;
        return this;
    }

    internal ComparisonSelector Build(string name)
    {
        if (!string.Equals(this.activity.Name, "any", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Only any-member cohort activity is currently supported.");
        }

        return ComparisonSelector.ForSources(this.sources).WithName(name);
    }
}
