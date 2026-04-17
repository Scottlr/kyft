using Kyft;

namespace Kyft.Internal.Definitions;

internal abstract class RollUpDefinition<TEvent>
    : WindowNodeDefinition<TEvent>
{
    private protected RollUpDefinition(string name)
    {
        Name = name;
        RollUps = [];
        Callbacks = new WindowCallbackSet<TEvent>();
    }

    public string Name { get; }

    public List<RollUpDefinition<TEvent>> RollUps { get; }

    public WindowCallbackSet<TEvent> Callbacks { get; }

    public virtual IEqualityComparer<object> KeyComparer { get; } =
        EqualityComparer<object>.Default;

    public abstract object GetKey(TEvent @event);

    public abstract bool IsActive(ChildActivityView children);
}

internal sealed class RollUpDefinition<TEvent, TKey> : RollUpDefinition<TEvent>
    where TKey : notnull
{
    public RollUpDefinition(
        string name,
        Func<TEvent, TKey> keySelector,
        Func<ChildActivityView, bool> isActiveSelector)
        : base(name)
    {
        KeySelector = keySelector;
        IsActiveSelector = isActiveSelector;
    }

    public Func<TEvent, TKey> KeySelector { get; }

    public Func<ChildActivityView, bool> IsActiveSelector { get; }

    public override object GetKey(TEvent @event)
    {
        return KeySelector(@event)
            ?? throw new InvalidOperationException(
                $"Roll-up window '{Name}' produced a null key.");
    }

    public override bool IsActive(ChildActivityView children)
    {
        return IsActiveSelector(children);
    }
}

internal sealed class DelegateRollUpDefinition<TEvent> : RollUpDefinition<TEvent>
{
    private readonly Func<TEvent, object> keySelector;
    private readonly Func<ChildActivityView, bool> isActiveSelector;

    public DelegateRollUpDefinition(
        string name,
        Func<TEvent, object> keySelector,
        IEqualityComparer<object> keyComparer,
        Func<ChildActivityView, bool> isActiveSelector)
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
                $"Roll-up window '{Name}' produced a null key.");
    }

    public override bool IsActive(ChildActivityView children)
    {
        return this.isActiveSelector(children);
    }
}
