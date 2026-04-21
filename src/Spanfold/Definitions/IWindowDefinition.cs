namespace Spanfold;

/// <summary>
/// Defines a reusable source window for an event pipeline.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public interface IWindowDefinition<TEvent>
{
    /// <summary>
    /// Configures the reusable source window.
    /// </summary>
    /// <param name="window">The source window definition builder.</param>
    void Define(WindowDefinitionBuilder<TEvent> window);
}
