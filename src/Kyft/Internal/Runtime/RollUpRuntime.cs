using Kyft.Internal.Definitions;
using Kyft.Internal.Keys;

namespace Kyft.Internal.Runtime;

internal sealed class RollUpRuntime<TEvent>
{
    private readonly RollUpDefinition<TEvent> definition;
    private readonly Dictionary<RuntimeStateKey, ParentState> parents;
    private readonly RollUpRuntime<TEvent>[] rollUps;

    public RollUpRuntime(RollUpDefinition<TEvent> definition)
    {
        this.definition = definition;
        this.parents = new Dictionary<RuntimeStateKey, ParentState>(
            new RuntimeStateKeyComparer(definition.KeyComparer));
        this.rollUps = new RollUpRuntime<TEvent>[definition.RollUps.Count];

        for (var i = 0; i < this.rollUps.Length; i++)
        {
            this.rollUps[i] = new RollUpRuntime<TEvent>(definition.RollUps[i]);
        }
    }

    public void ObserveChild(
        TEvent @event,
        object? source,
        object? partition,
        object childKey,
        bool childIsActive,
        bool childChanged,
        ref List<WindowEmission<TEvent>>? emissions)
    {
        var parentKey = this.definition.GetKey(@event);
        var parentStateKey = new RuntimeStateKey(parentKey, source, partition);

        if (!this.parents.TryGetValue(parentStateKey, out var parent))
        {
            parent = new ParentState();
            this.parents.Add(parentStateKey, parent);
        }

        parent.Children[childKey] = childIsActive;

        var parentChanged = false;

        if (!childChanged)
        {
            PropagateToParents(
                @event,
                source,
                partition,
                parentKey,
                parent.IsActive,
                parentChanged,
                ref emissions);
            return;
        }

        var children = parent.ToChildActivityView();
        var isActive = this.definition.IsActive(children);

        if (isActive == parent.IsActive)
        {
            PropagateToParents(
                @event,
                source,
                partition,
                parentKey,
                parent.IsActive,
                parentChanged,
                ref emissions);
            return;
        }

        parent.IsActive = isActive;
        parentChanged = true;
        WindowRuntime<TEvent>.AddEmission(
            ref emissions,
            new WindowEmission<TEvent>(
                this.definition.Name,
                parentKey,
                @event,
                isActive ? WindowTransitionKind.Opened : WindowTransitionKind.Closed,
                source,
                partition));

        PropagateToParents(
            @event,
            source,
            partition,
            parentKey,
            parent.IsActive,
            parentChanged,
            ref emissions);
    }

    private void PropagateToParents(
        TEvent @event,
        object? source,
        object? partition,
        object parentKey,
        bool parentIsActive,
        bool parentChanged,
        ref List<WindowEmission<TEvent>>? emissions)
    {
        foreach (var rollUp in this.rollUps)
        {
            rollUp.ObserveChild(
                @event,
                source,
                partition,
                parentKey,
                parentIsActive,
                parentChanged,
                ref emissions);
        }
    }

    private sealed class ParentState
    {
        public Dictionary<object, bool> Children { get; } = [];

        public bool IsActive { get; set; }

        public ChildActivityView ToChildActivityView()
        {
            var activeCount = 0;

            foreach (var child in Children)
            {
                if (child.Value)
                {
                    activeCount++;
                }
            }

            return new ChildActivityView(activeCount, Children.Count);
        }
    }
}
