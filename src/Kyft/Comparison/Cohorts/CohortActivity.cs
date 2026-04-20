namespace Kyft;

/// <summary>
/// Describes how a cohort is considered active.
/// </summary>
/// <remarks>
/// The first cohort implementation supports <see cref="Any" />, where the
/// cohort is active when at least one declared member source is active.
/// </remarks>
public sealed record CohortActivity
{
    private CohortActivity(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Gets the activity rule name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Creates an activity rule where any active member makes the cohort active.
    /// </summary>
    /// <returns>An any-member activity rule.</returns>
    public static CohortActivity Any()
    {
        return new CohortActivity("any");
    }
}
