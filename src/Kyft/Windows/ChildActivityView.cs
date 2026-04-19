namespace Kyft;

/// <summary>
/// Read-only activity summary passed to roll-up predicates.
/// </summary>
public sealed class ChildActivityView
{
    internal ChildActivityView(int activeCount, int totalCount)
    {
        ActiveCount = activeCount;
        TotalCount = totalCount;
    }

    /// <summary>
    /// Gets the number of active child windows.
    /// </summary>
    public int ActiveCount { get; }

    /// <summary>
    /// Gets the number of known child windows.
    /// </summary>
    public int TotalCount { get; }

    /// <summary>
    /// Returns true when at least one child exists and every known child is active.
    /// </summary>
    /// <returns>True when all known children are active.</returns>
    public bool AllActive()
    {
        return TotalCount > 0 && ActiveCount == TotalCount;
    }

    /// <summary>
    /// Returns true when at least one known child is active.
    /// </summary>
    /// <returns>True when any child is active.</returns>
    public bool AnyActive()
    {
        return ActiveCount > 0;
    }

    /// <summary>
    /// Deconstructs the view into active and total child counts.
    /// </summary>
    /// <param name="activeCount">The number of active child windows.</param>
    /// <param name="totalCount">The number of known child windows.</param>
    public void Deconstruct(out int activeCount, out int totalCount)
    {
        activeCount = ActiveCount;
        totalCount = TotalCount;
    }
}
