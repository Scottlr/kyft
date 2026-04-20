using Kyft.Internal.Definitions;
using Kyft.Internal.Keys;

namespace Kyft;

/// <summary>
/// Configures a reusable source window definition.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public sealed class WindowDefinitionBuilder<TEvent>
{
    private readonly WindowCallbackSet<TEvent> callbacks;
    private string name;
    private Func<TEvent, object>? keySelector;
    private IEqualityComparer<object>? keyComparer;
    private Func<TEvent, bool>? isActiveSelector;
    private readonly List<SegmentBuilder<TEvent>> segments;

    internal WindowDefinitionBuilder(string defaultName)
    {
        this.name = defaultName;
        this.callbacks = new WindowCallbackSet<TEvent>();
        this.segments = [];
    }

    /// <summary>
    /// Sets the public window name. Defaults to the definition type name.
    /// </summary>
    /// <param name="name">The unique window name.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> Named(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.name = name;
        return this;
    }

    /// <summary>
    /// Sets the logical key selector for the window.
    /// </summary>
    /// <typeparam name="TKey">The key type used to track independent window state.</typeparam>
    /// <param name="key">Selects the logical key from each event.</param>
    /// <param name="comparer">Optional comparer for source window keys.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> Key<TKey>(
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
    /// Sets the active-state selector for the window.
    /// </summary>
    /// <param name="isActive">Returns true when the keyed window should be active.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> ActiveWhen(Func<TEvent, bool> isActive)
    {
        ArgumentNullException.ThrowIfNull(isActive);

        this.isActiveSelector = isActive;
        return this;
    }

    /// <summary>
    /// Adds an analytical segment dimension that can split active windows when its value changes.
    /// </summary>
    /// <param name="name">The segment dimension name.</param>
    /// <param name="configure">Configures the segment value and optional child segments.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> Segment(
        string name,
        Action<SegmentBuilder<TEvent>> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var segment = new SegmentBuilder<TEvent>(name);
        configure(segment);
        this.segments.Add(segment);
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when this window opens.
    /// </summary>
    /// <param name="callback">The callback to invoke for open emissions.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> OnOpened(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Opened.Add(callback);
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when this window closes.
    /// </summary>
    /// <param name="callback">The callback to invoke for close emissions.</param>
    /// <returns>The current builder.</returns>
    public WindowDefinitionBuilder<TEvent> OnClosed(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Closed.Add(callback);
        return this;
    }

    internal WindowDefinition<TEvent> Build()
    {
        if (this.keySelector is null)
        {
            throw new InvalidOperationException(
                $"Window definition '{this.name}' must configure a key.");
        }

        if (this.isActiveSelector is null)
        {
            throw new InvalidOperationException(
                $"Window definition '{this.name}' must configure active state.");
        }

        var definition = new DelegateWindowDefinition<TEvent>(
            this.name,
            this.keySelector,
            this.keyComparer ?? EqualityComparer<object>.Default,
            this.isActiveSelector,
            BuildSegments());

        definition.Callbacks.Opened.AddRange(this.callbacks.Opened);
        definition.Callbacks.Closed.AddRange(this.callbacks.Closed);

        return definition;
    }

    private IReadOnlyList<SegmentDefinition<TEvent>> BuildSegments()
    {
        if (this.segments.Count == 0)
        {
            return [];
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        var definitions = new SegmentDefinition<TEvent>[this.segments.Count];
        for (var i = 0; i < definitions.Length; i++)
        {
            definitions[i] = this.segments[i].Build(names);
        }

        return definitions;
    }
}
