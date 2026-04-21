using Spanfold.Internal.Definitions;

namespace Spanfold;

/// <summary>
/// Configures optional behavior for one source window.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
/// <typeparam name="TKey">The key type used by the source window.</typeparam>
public sealed class WindowOptions<TEvent, TKey>
    where TKey : notnull
{
    private readonly WindowCallbackSet<TEvent> callbacks;

    internal WindowOptions(WindowCallbackSet<TEvent> callbacks)
    {
        this.callbacks = callbacks;
    }

    /// <summary>
    /// Registers a callback invoked when this window opens.
    /// </summary>
    /// <param name="callback">The callback to invoke for open emissions.</param>
    /// <returns>The current options object.</returns>
    public WindowOptions<TEvent, TKey> OnOpened(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Opened.Add(callback);
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when this window closes.
    /// </summary>
    /// <param name="callback">The callback to invoke for close emissions.</param>
    /// <returns>The current options object.</returns>
    public WindowOptions<TEvent, TKey> OnClosed(Action<WindowEmission<TEvent>> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        this.callbacks.Closed.Add(callback);
        return this;
    }
}
