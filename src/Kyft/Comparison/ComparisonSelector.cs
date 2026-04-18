namespace Kyft;

/// <summary>
/// Describes a selection used by a window comparison plan.
/// </summary>
/// <param name="Name">The selector name used in output and diagnostics.</param>
/// <param name="Description">A readable description of the selector.</param>
/// <param name="IsSerializable">Whether the selector can be exported as plan data.</param>
public readonly record struct ComparisonSelector(
    string Name,
    string Description,
    bool IsSerializable)
{
    /// <summary>
    /// Creates a serializable selector descriptor.
    /// </summary>
    /// <param name="name">The selector name.</param>
    /// <param name="description">A readable selector description.</param>
    /// <returns>A serializable comparison selector.</returns>
    public static ComparisonSelector Serializable(string name, string description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);

        return new ComparisonSelector(name, description, IsSerializable: true);
    }

    /// <summary>
    /// Creates a runtime-only selector descriptor.
    /// </summary>
    /// <param name="name">The selector name.</param>
    /// <param name="description">A readable selector description.</param>
    /// <returns>A runtime-only comparison selector.</returns>
    public static ComparisonSelector RuntimeOnly(string name, string description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);

        return new ComparisonSelector(name, description, IsSerializable: false);
    }
}
