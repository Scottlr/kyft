using Kyft.Internal.Keys;

namespace Kyft.Internal.Definitions;

internal abstract class WindowDefinition<TEvent> : WindowNodeDefinition<TEvent>
{
    private protected WindowDefinition(string name)
    {
        Name = name;
        RollUps = [];
        Callbacks = new WindowCallbackSet<TEvent>();
    }

    public string Name { get; }

    public List<RollUpDefinition<TEvent>> RollUps { get; }

    public WindowCallbackSet<TEvent> Callbacks { get; }

    public abstract IEqualityComparer<object> KeyComparer { get; }

    public abstract object GetKey(TEvent @event);

    public abstract bool IsActive(TEvent @event);
}

internal sealed class WindowDefinition<TEvent, TKey> : WindowDefinition<TEvent>
    where TKey : notnull
{
    public WindowDefinition(
        string name,
        Func<TEvent, TKey> keySelector,
        Func<TEvent, bool> isActiveSelector,
        IEqualityComparer<TKey>? comparer)
        : base(name)
    {
        KeySelector = keySelector;
        IsActiveSelector = isActiveSelector;
        KeyComparer = new ObjectKeyComparer<TKey>(
            comparer ?? EqualityComparer<TKey>.Default);
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
        Func<TEvent, bool> isActiveSelector)
        : base(name)
    {
        this.keySelector = keySelector;
        KeyComparer = keyComparer;
        this.isActiveSelector = isActiveSelector;
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
