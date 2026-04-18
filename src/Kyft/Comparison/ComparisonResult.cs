namespace Kyft;

/// <summary>
/// Represents the current comparison result.
/// </summary>
/// <remarks>
/// This initial result shell carries the plan and validation diagnostics. Later
/// specs will extend it with normalized windows, aligned segments, comparator
/// rows, summaries, and explain output.
/// </remarks>
/// <param name="Plan">The comparison plan.</param>
/// <param name="Diagnostics">The validation diagnostics.</param>
/// <param name="Prepared">The prepared comparison artifact, when execution reached preparation.</param>
/// <param name="Aligned">The aligned comparison artifact, when execution reached alignment.</param>
/// <param name="ComparatorSummaries">The comparator summaries.</param>
/// <param name="OverlapRows">Rows emitted by the overlap comparator.</param>
/// <param name="ResidualRows">Rows emitted by the residual comparator.</param>
/// <param name="MissingRows">Rows emitted by the missing comparator.</param>
/// <param name="CoverageRows">Rows emitted by the coverage comparator.</param>
/// <param name="CoverageSummaries">Summaries emitted by the coverage comparator.</param>
public sealed record ComparisonResult(
    ComparisonPlan Plan,
    IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics,
    PreparedComparison? Prepared = null,
    AlignedComparison? Aligned = null,
    IReadOnlyList<ComparatorSummary>? ComparatorSummaries = null,
    IReadOnlyList<OverlapRow>? OverlapRows = null,
    IReadOnlyList<ResidualRow>? ResidualRows = null,
    IReadOnlyList<MissingRow>? MissingRows = null,
    IReadOnlyList<CoverageRow>? CoverageRows = null,
    IReadOnlyList<CoverageSummary>? CoverageSummaries = null)
{
    /// <summary>
    /// Gets whether the plan produced no validation diagnostics.
    /// </summary>
    public bool IsValid => Diagnostics.All(static diagnostic =>
        diagnostic.Severity != ComparisonPlanDiagnosticSeverity.Error);
}
