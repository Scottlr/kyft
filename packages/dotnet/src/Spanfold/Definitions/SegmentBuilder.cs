using Spanfold.Internal.Definitions;

namespace Spanfold;

/// <summary>
/// Builds one segment dimension in a nested segment hierarchy.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public sealed class SegmentBuilder<TEvent>
{
    private readonly string name;
    private readonly string? parentName;
    private readonly List<SegmentBuilder<TEvent>> children;
    private Func<TEvent, object?>? valueSelector;

    internal SegmentBuilder(string name, string? parentName = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.name = name;
        this.parentName = parentName;
        this.children = [];
    }

    /// <summary>
    /// Sets the segment value selector.
    /// </summary>
    /// <typeparam name="TValue">The segment value type.</typeparam>
    /// <param name="value">Selects the segment value from an event.</param>
    /// <returns>The current builder.</returns>
    public SegmentBuilder<TEvent> Value<TValue>(Func<TEvent, TValue> value)
    {
        ArgumentNullException.ThrowIfNull(value);

        this.valueSelector = @event => value(@event);
        return this;
    }

    /// <summary>
    /// Adds a child segment under this segment.
    /// </summary>
    /// <param name="name">The child segment name.</param>
    /// <param name="configure">Configures the child segment.</param>
    /// <returns>The current builder.</returns>
    public SegmentBuilder<TEvent> Child(
        string name,
        Action<SegmentBuilder<TEvent>> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var child = new SegmentBuilder<TEvent>(name, this.name);
        configure(child);
        this.children.Add(child);
        return this;
    }

    internal SegmentDefinition<TEvent> Build(HashSet<string> names)
    {
        if (!names.Add(this.name))
        {
            throw new InvalidOperationException(
                $"A segment named '{this.name}' has already been configured.");
        }

        if (this.valueSelector is null)
        {
            throw new InvalidOperationException(
                $"Segment '{this.name}' must configure a value selector.");
        }

        var childDefinitions = new SegmentDefinition<TEvent>[this.children.Count];
        for (var i = 0; i < childDefinitions.Length; i++)
        {
            childDefinitions[i] = this.children[i].Build(names);
        }

        return new SegmentDefinition<TEvent>(
            this.name,
            this.parentName,
            this.valueSelector,
            childDefinitions);
    }
}
