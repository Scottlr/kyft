namespace Kyft;

/// <summary>
/// Defines a reusable roll-up window for an event pipeline.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public interface IRollUpDefinition<TEvent>
{
    /// <summary>
    /// Configures the reusable roll-up window.
    /// </summary>
    /// <param name="rollUp">The roll-up definition builder.</param>
    void Define(RollUpDefinitionBuilder<TEvent> rollUp);
}
