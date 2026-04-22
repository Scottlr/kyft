namespace Spanfold.Internal.Definitions;

internal sealed class WindowCallbackSet<TEvent>
{
    public List<Action<WindowEmission<TEvent>>> Opened { get; } = [];

    public List<Action<WindowEmission<TEvent>>> Closed { get; } = [];

    public bool HasCallbacks => Opened.Count > 0 || Closed.Count > 0;

    public WindowCallbackSet<TEvent> Copy()
    {
        var copy = new WindowCallbackSet<TEvent>();
        copy.Opened.AddRange(Opened);
        copy.Closed.AddRange(Closed);

        return copy;
    }
}
