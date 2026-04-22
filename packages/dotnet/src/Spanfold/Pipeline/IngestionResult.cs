namespace Spanfold;

/// <summary>
/// Contains the emissions produced by an ingestion operation.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
/// <param name="emissions">The emissions produced by ingestion.</param>
public sealed class IngestionResult<TEvent>(IReadOnlyList<WindowEmission<TEvent>> emissions)
{
    /// <summary>
    /// Gets the emissions produced by ingestion.
    /// </summary>
    public IReadOnlyList<WindowEmission<TEvent>> Emissions { get; } = emissions;

    /// <summary>
    /// Gets whether any emissions were produced.
    /// </summary>
    public bool HasEmissions => Emissions.Count > 0;

    /// <summary>
    /// Deconstructs the result into emissions and emission presence.
    /// </summary>
    /// <param name="emissions">The emissions produced by ingestion.</param>
    /// <param name="hasEmissions">Whether any emissions were produced.</param>
    public void Deconstruct(
        out IReadOnlyList<WindowEmission<TEvent>> emissions,
        out bool hasEmissions)
    {
        emissions = Emissions;
        hasEmissions = HasEmissions;
    }
}
