using Kyft.Internal.Keys;

namespace Kyft.Internal.Definitions;

internal abstract class WindowDefinition<TEvent> : WindowNodeDefinition<TEvent>
{
    private protected WindowDefinition(string name)
    {
        Name = name;
        RollUps = [];
        Callbacks = new WindowCallbackSet<TEvent>();
        SegmentDefinitions = [];
    }

    public string Name { get; }

    public List<RollUpDefinition<TEvent>> RollUps { get; }

    public WindowCallbackSet<TEvent> Callbacks { get; }

    public IReadOnlyList<SegmentDefinition<TEvent>> SegmentDefinitions { get; private protected set; }

    public abstract IEqualityComparer<object> KeyComparer { get; }

    public abstract object GetKey(TEvent @event);

    public abstract bool IsActive(TEvent @event);

    public IReadOnlyList<WindowSegment> GetSegments(TEvent @event)
    {
        if (SegmentDefinitions.Count == 0)
        {
            return [];
        }

        var segments = new List<WindowSegment>(SegmentDefinitions.Count);
        for (var i = 0; i < SegmentDefinitions.Count; i++)
        {
            AddSegment(@event, SegmentDefinitions[i], segments);
        }

        return segments.ToArray();
    }

    private static void AddSegment(
        TEvent @event,
        SegmentDefinition<TEvent> definition,
        List<WindowSegment> segments)
    {
        segments.Add(new WindowSegment(
            definition.Name,
            definition.ValueSelector(@event),
            definition.ParentName));

        for (var i = 0; i < definition.Children.Count; i++)
        {
            AddSegment(@event, definition.Children[i], segments);
        }
    }
}

internal sealed class WindowDefinition<TEvent, TKey> : WindowDefinition<TEvent>
    where TKey : notnull
{
    public WindowDefinition(
        string name,
        Func<TEvent, TKey> keySelector,
        Func<TEvent, bool> isActiveSelector,
        IEqualityComparer<TKey>? comparer,
        IReadOnlyList<SegmentDefinition<TEvent>>? segmentDefinitions = null)
        : base(name)
    {
        KeySelector = keySelector;
        IsActiveSelector = isActiveSelector;
        KeyComparer = new ObjectKeyComparer<TKey>(
            comparer ?? EqualityComparer<TKey>.Default);
        SegmentDefinitions = segmentDefinitions ?? [];
    }

    public Func<TEvent, TKey> KeySelector { get; }

    public Func<TEvent, bool> IsActiveSelector { get; }

    public override IEqualityComparer<object> KeyComparer { get; }

    public override object GetKey(TEvent @event)
    {
        return KeySelector(@event)
            ?? throw new InvalidOperationException(
                $"Window '{Name}' produced a null key.");
    }

    public override bool IsActive(TEvent @event)
    {
        return IsActiveSelector(@event);
    }
}

internal sealed class DelegateWindowDefinition<TEvent> : WindowDefinition<TEvent>
{
    private readonly Func<TEvent, object> keySelector;
    private readonly Func<TEvent, bool> isActiveSelector;

    public DelegateWindowDefinition(
        string name,
        Func<TEvent, object> keySelector,
        IEqualityComparer<object> keyComparer,
        Func<TEvent, bool> isActiveSelector,
        IReadOnlyList<SegmentDefinition<TEvent>>? segmentDefinitions = null)
        : base(name)
    {
        this.keySelector = keySelector;
        KeyComparer = keyComparer;
        this.isActiveSelector = isActiveSelector;
        SegmentDefinitions = segmentDefinitions ?? [];
    }

    public override IEqualityComparer<object> KeyComparer { get; }

    public override object GetKey(TEvent @event)
    {
        return this.keySelector(@event)
            ?? throw new InvalidOperationException(
                $"Window '{Name}' produced a null key.");
    }

    public override bool IsActive(TEvent @event)
    {
        return this.isActiveSelector(@event);
    }
}
