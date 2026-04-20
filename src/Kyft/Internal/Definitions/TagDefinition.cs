namespace Kyft.Internal.Definitions;

internal sealed class TagDefinition<TEvent>
{
    public TagDefinition(string name, Func<TEvent, object?> valueSelector)
    {
        Name = name;
        ValueSelector = valueSelector;
    }

    public string Name { get; }

    public Func<TEvent, object?> ValueSelector { get; }
}
