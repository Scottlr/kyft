namespace Kyft.Internal.Definitions;

internal interface WindowNodeDefinition<TEvent>
{
    string Name { get; }

    List<RollUpDefinition<TEvent>> RollUps { get; }

    WindowCallbackSet<TEvent> Callbacks { get; }
}
