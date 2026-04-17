namespace Kyft;

/// <summary>
/// Entry point for creating Kyft event pipelines.
/// </summary>
public static class Kyft
{
    /// <summary>
    /// Starts a pipeline definition for events of type <typeparamref name="TEvent"/>.
    /// </summary>
    /// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
    /// <returns>A builder for configuring state windows over the event type.</returns>
    public static EventPipelineBuilder<TEvent> For<TEvent>()
    {
        return new EventPipelineBuilder<TEvent>();
    }
}
