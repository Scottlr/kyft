namespace Kyft;

/// <summary>
/// Builds a staged comparison over recorded window history.
/// </summary>
/// <remarks>
/// The builder captures the comparison question without mutating or enumerating
/// the source history. Later stages add target, comparison sources,
/// normalization, and comparator choices before execution.
/// </remarks>
public sealed class WindowComparisonBuilder
{
    internal WindowComparisonBuilder(WindowIntervalHistory history, string name)
    {
        History = history;
        Name = name;
    }

    /// <summary>
    /// Gets the user-supplied comparison name.
    /// </summary>
    public string Name { get; }

    internal WindowIntervalHistory History { get; }
}
