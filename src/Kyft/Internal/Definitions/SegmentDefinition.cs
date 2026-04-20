namespace Kyft.Internal.Definitions;

internal sealed class SegmentDefinition<TEvent>
{
    public SegmentDefinition(
        string name,
        string? parentName,
        Func<TEvent, object?> valueSelector,
        IReadOnlyList<SegmentDefinition<TEvent>> children)
    {
        Name = name;
        ParentName = parentName;
        ValueSelector = valueSelector;
        Children = children;
    }

    public string Name { get; }

    public string? ParentName { get; }

    public Func<TEvent, object?> ValueSelector { get; }

    public IReadOnlyList<SegmentDefinition<TEvent>> Children { get; }
}
