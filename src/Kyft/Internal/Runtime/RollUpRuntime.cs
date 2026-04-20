using Kyft.Internal.Definitions;
using Kyft.Internal.Keys;

namespace Kyft.Internal.Runtime;

internal sealed class RollUpRuntime<TEvent>
{
    private readonly RollUpDefinition<TEvent> definition;
    private readonly Dictionary<RollUpStateKey, ParentState> parents;
    private readonly RollUpRuntime<TEvent>[] rollUps;

    public RollUpRuntime(RollUpDefinition<TEvent> definition)
    {
        this.definition = definition;
        this.parents = new Dictionary<RollUpStateKey, ParentState>(
            new RollUpStateKeyComparer(definition.KeyComparer));
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
        IReadOnlyList<WindowSegment> segments,
        IReadOnlyList<WindowTag> tags,
        ref List<WindowEmission<TEvent>>? emissions)
    {
        var parentKey = this.definition.GetKey(@event);
        var parentStateKey = new RollUpStateKey(
            parentKey,
            source,
            partition,
            StableSegments(segments));

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
                segments,
                tags,
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
                segments,
                tags,
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
                partition,
                segments,
                tags,
                isActive ? null : WindowBoundaryReason.ActivePredicateEnded));

        PropagateToParents(
            @event,
            source,
            partition,
            parentKey,
            parent.IsActive,
            parentChanged,
            segments,
            tags,
            ref emissions);
    }

    private void PropagateToParents(
        TEvent @event,
        object? source,
        object? partition,
        object parentKey,
        bool parentIsActive,
        bool parentChanged,
        IReadOnlyList<WindowSegment> segments,
        IReadOnlyList<WindowTag> tags,
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
                segments,
                tags,
                ref emissions);
        }
    }

    private static string StableSegments(IReadOnlyList<WindowSegment> segments)
    {
        if (segments.Count == 0)
        {
            return string.Empty;
        }

        var builder = new System.Text.StringBuilder();
        for (var i = 0; i < segments.Count; i++)
        {
            var segment = segments[i];
            builder
                .Append(segment.ParentName ?? string.Empty)
                .Append('/')
                .Append(segment.Name)
                .Append('=')
                .Append(segment.Value)
                .Append(';');
        }

        return builder.ToString();
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

    private readonly record struct RollUpStateKey(
        object Key,
        object? Source,
        object? Partition,
        string SegmentContext);

    private sealed class RollUpStateKeyComparer : IEqualityComparer<RollUpStateKey>
    {
        private readonly IEqualityComparer<object> keyComparer;

        public RollUpStateKeyComparer(IEqualityComparer<object> keyComparer)
        {
            this.keyComparer = keyComparer;
        }

        public bool Equals(RollUpStateKey x, RollUpStateKey y)
        {
            return this.keyComparer.Equals(x.Key, y.Key)
                && EqualityComparer<object?>.Default.Equals(x.Source, y.Source)
                && EqualityComparer<object?>.Default.Equals(x.Partition, y.Partition)
                && string.Equals(x.SegmentContext, y.SegmentContext, StringComparison.Ordinal);
        }

        public int GetHashCode(RollUpStateKey obj)
        {
            return HashCode.Combine(
                this.keyComparer.GetHashCode(obj.Key),
                obj.Source,
                obj.Partition,
                obj.SegmentContext);
        }
    }
}
