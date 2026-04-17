namespace Kyft;

/// <summary>
/// Describes a window open or close transition produced by ingestion.
/// </summary>
/// <typeparam name="TEvent">The event type consumed by the pipeline.</typeparam>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical key whose window changed state.</param>
/// <param name="Event">The event that produced the transition.</param>
/// <param name="Kind">The transition kind.</param>
/// <param name="Source">Optional source identity supplied during ingestion.</param>
/// <param name="Partition">Optional partition identity supplied during ingestion.</param>
public sealed record WindowEmission<TEvent>(
    string WindowName,
    object Key,
    TEvent Event,
    WindowTransitionKind Kind,
    object? Source = null,
    object? Partition = null);
