using Kyft.Internal.Definitions;
using Kyft.Internal.Keys;

namespace Kyft;

/// <summary>
/// Configures a reusable roll-up window definition.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public sealed class RollUpDefinitionBuilder<TEvent>
{
    private readonly WindowCallbackSet<TEvent> callbacks;
    private string name;
    private Func<TEvent, object>? keySelector;
    private IEqualityComparer<object>? keyComparer;
    private Func<ChildActivityView, bool>? isActiveSelector;
    private RollUpSegmentProjection segmentProjection = RollUpSegmentProjection.PreserveAll;

    internal RollUpDefinitionBuilder(string defaultName)
    {
        this.name = defaultName;
        this.callbacks = new WindowCallbackSet<TEvent>();
    }

    /// <summary>
    /// Sets the public roll-up name. Defaults to the definition type name.
    /// </summary>
    /// <param name="name">The unique roll-up window name.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> Named(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.name = name;
        return this;
    }

    /// <summary>
    /// Sets the parent key selector for the roll-up.
    /// </summary>
    /// <typeparam name="TKey">The parent key type.</typeparam>
    /// <param name="key">Selects the parent key from each event.</param>
    /// <param name="comparer">Optional comparer for parent keys.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> Key<TKey>(
        Func<TEvent, TKey> key,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(key);

        this.keySelector = @event => key(@event)!;
        this.keyComparer = new ObjectKeyComparer<TKey>(
            comparer ?? EqualityComparer<TKey>.Default);
        return this;
    }

    /// <summary>
    /// Sets the active-state selector for the roll-up.
    /// </summary>
    /// <param name="isActive">Returns true when the parent should be active for its children.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> ActiveWhen(
        Func<ChildActivityView, bool> isActive)
    {
        ArgumentNullException.ThrowIfNull(isActive);

        this.isActiveSelector = isActive;
        return this;
    }

    /// <summary>
    /// Configures which child segment dimensions are preserved by the roll-up.
    /// </summary>
    /// <param name="configure">Configures the roll-up segment projection.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> Segments(
        Action<RollUpSegmentProjectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new RollUpSegmentProjectionBuilder();
        configure(builder);
        this.segmentProjection = builder.Build();
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when this roll-up opens.
    /// </summary>
    /// <param name="callback">The callback to invoke for open emissions.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> OnOpened(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Opened.Add(callback);
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when this roll-up closes.
    /// </summary>
    /// <param name="callback">The callback to invoke for close emissions.</param>
    /// <returns>The current builder.</returns>
    public RollUpDefinitionBuilder<TEvent> OnClosed(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Closed.Add(callback);
        return this;
    }

    internal RollUpDefinition<TEvent> Build()
    {
        if (this.keySelector is null)
        {
            throw new InvalidOperationException(
                $"Roll-up definition '{this.name}' must configure a key.");
        }

        if (this.isActiveSelector is null)
        {
            throw new InvalidOperationException(
                $"Roll-up definition '{this.name}' must configure active state.");
        }

        var definition = new DelegateRollUpDefinition<TEvent>(
            this.name,
            this.keySelector,
            this.keyComparer ?? EqualityComparer<object>.Default,
            this.isActiveSelector,
            this.segmentProjection);

        definition.Callbacks.Opened.AddRange(this.callbacks.Opened);
        definition.Callbacks.Closed.AddRange(this.callbacks.Closed);

        return definition;
    }
}
