namespace Kyft;

/// <summary>
/// Describes finality metadata for a materialized comparison row.
/// </summary>
/// <param name="RowType">The exported row family, such as overlap or residual.</param>
/// <param name="RowId">The deterministic row identifier inside that row family.</param>
/// <param name="Finality">Whether the row is final or provisional.</param>
/// <param name="Reason">A short human-readable finality reason.</param>
public sealed record ComparisonRowFinality(
    string RowType,
    string RowId,
    ComparisonFinality Finality,
    string Reason);
