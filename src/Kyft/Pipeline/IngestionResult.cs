namespace Kyft;

/// <summary>
/// Contains the emissions produced by an ingestion operation.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
public sealed class IngestionResult<TEvent>
{
    /// <summary>
    /// Creates an ingestion result.
    /// </summary>
    /// <param name="emissions">The emissions produced by ingestion.</param>
    public IngestionResult(IReadOnlyList<WindowEmission<TEvent>> emissions)
    {
        Emissions = emissions;
    }

    /// <summary>
    /// Gets the emissions produced by ingestion.
    /// </summary>
    public IReadOnlyList<WindowEmission<TEvent>> Emissions { get; }

    /// <summary>
    /// Gets whether any emissions were produced.
    /// </summary>
    public bool HasEmissions => Emissions.Count > 0;
}
