namespace Kyft;

/// <summary>
/// Describes finality metadata for a materialized comparison row.
/// </summary>
/// <param name="RowType">The exported row family, such as overlap or residual.</param>
/// <param name="RowId">The deterministic row identifier inside that row family.</param>
/// <param name="Finality">Whether the row is final or provisional.</param>
/// <param name="Reason">A short human-readable finality reason.</param>
/// <param name="Version">The deterministic row metadata version.</param>
/// <param name="SupersedesRowId">The prior row identifier superseded by this metadata, when any.</param>
public sealed record ComparisonRowFinality(
    string RowType,
    string RowId,
    ComparisonFinality Finality,
    string Reason,
    int Version = 1,
    string? SupersedesRowId = null);
