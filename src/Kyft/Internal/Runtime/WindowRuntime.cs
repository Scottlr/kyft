using Kyft.Internal.Definitions;
using Kyft.Internal.Keys;

namespace Kyft.Internal.Runtime;

internal sealed class WindowRuntime<TEvent>
{
    private readonly WindowDefinition<TEvent> definition;
    private readonly HashSet<RuntimeStateKey> activeKeys;
    private readonly RollUpRuntime<TEvent>[] rollUps;

    public WindowRuntime(WindowDefinition<TEvent> definition)
    {
        this.definition = definition;
        this.activeKeys = new HashSet<RuntimeStateKey>(
            new RuntimeStateKeyComparer(definition.KeyComparer));
        this.rollUps = new RollUpRuntime<TEvent>[definition.RollUps.Count];

        for (var i = 0; i < this.rollUps.Length; i++)
        {
            this.rollUps[i] = new RollUpRuntime<TEvent>(definition.RollUps[i]);
        }
    }

    public void Ingest(
        TEvent @event,
        object? source,
        object? partition,
        ref List<WindowEmission<TEvent>>? emissions)
    {
        var key = this.definition.GetKey(@event);
        var isActive = this.definition.IsActive(@event);
        var stateKey = new RuntimeStateKey(key, source, partition);
        var wasActive = this.activeKeys.Contains(stateKey);
        var changed = isActive != wasActive;

        if (changed && isActive)
        {
            this.activeKeys.Add(stateKey);
            AddEmission(
                ref emissions,
                new WindowEmission<TEvent>(
                    this.definition.Name,
                    key,
                    @event,
                    WindowTransitionKind.Opened,
                    source,
                    partition));
        }
        else if (changed)
        {
            this.activeKeys.Remove(stateKey);
            AddEmission(
                ref emissions,
                new WindowEmission<TEvent>(
                    this.definition.Name,
                    key,
                    @event,
                    WindowTransitionKind.Closed,
                    source,
                    partition));
        }

        foreach (var rollUp in this.rollUps)
        {
            rollUp.ObserveChild(
                @event,
                source,
                partition,
                key,
                isActive,
                changed,
                ref emissions);
        }
    }

    internal static void AddEmission(
        ref List<WindowEmission<TEvent>>? emissions,
        WindowEmission<TEvent> emission)
    {
        emissions ??= [];
        emissions.Add(emission);
    }
}
