using Kyft.Internal.Definitions;

namespace Kyft;

/// <summary>
/// Configures which child segment dimensions are preserved by a roll-up window.
/// </summary>
/// <remarks>
/// Roll-ups preserve every child segment by default. Use this builder when a
/// parent window should intentionally ignore lower-level dimensions, such as
/// preserving a phase while dropping an operational state inside that phase.
/// </remarks>
public sealed class RollUpSegmentProjectionBuilder
{
    private readonly HashSet<string> preservedNames = new(StringComparer.Ordinal);
    private readonly HashSet<string> droppedNames = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> renamedNames = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Func<object?, object?>> valueTransforms = new(StringComparer.Ordinal);

    internal RollUpSegmentProjectionBuilder()
    {
    }

    /// <summary>
    /// Preserves the named segment dimension on the roll-up.
    /// </summary>
    /// <param name="name">The segment dimension name to keep.</param>
    /// <returns>The current builder.</returns>
    public RollUpSegmentProjectionBuilder Preserve(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (this.droppedNames.Contains(name))
        {
            throw new InvalidOperationException(
                $"Segment '{name}' cannot be both preserved and dropped.");
        }

        this.preservedNames.Add(name);
        return this;
    }

    /// <summary>
    /// Drops the named segment dimension from the roll-up.
    /// </summary>
    /// <param name="name">The segment dimension name to remove.</param>
    /// <returns>The current builder.</returns>
    public RollUpSegmentProjectionBuilder Drop(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (this.preservedNames.Contains(name))
        {
            throw new InvalidOperationException(
                $"Segment '{name}' cannot be both preserved and dropped.");
        }

        if (this.renamedNames.ContainsKey(name) || this.valueTransforms.ContainsKey(name))
        {
            throw new InvalidOperationException(
                $"Segment '{name}' cannot be dropped after it has been projected.");
        }

        this.droppedNames.Add(name);
        return this;
    }

    /// <summary>
    /// Renames a child segment dimension on the roll-up.
    /// </summary>
    /// <param name="name">The child segment dimension name.</param>
    /// <param name="projectedName">The segment dimension name to emit on the roll-up.</param>
    /// <returns>The current builder.</returns>
    public RollUpSegmentProjectionBuilder Rename(
        string name,
        string projectedName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectedName);

        ThrowIfDropped(name);
        this.renamedNames[name] = projectedName;
        return this;
    }

    /// <summary>
    /// Transforms a child segment value before it is emitted on the roll-up.
    /// </summary>
    /// <param name="name">The child segment dimension name.</param>
    /// <param name="transform">Transforms the child segment value.</param>
    /// <returns>The current builder.</returns>
    public RollUpSegmentProjectionBuilder Transform(
        string name,
        Func<object?, object?> transform)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(transform);

        ThrowIfDropped(name);
        this.valueTransforms[name] = transform;
        return this;
    }

    internal RollUpSegmentProjection Build()
    {
        return new RollUpSegmentProjection(
            this.preservedNames,
            this.droppedNames,
            this.renamedNames,
            this.valueTransforms);
    }

    private void ThrowIfDropped(string name)
    {
        if (this.droppedNames.Contains(name))
        {
            throw new InvalidOperationException(
                $"Segment '{name}' cannot be projected after it has been dropped.");
        }
    }
}
