namespace Kyft;

/// <summary>
/// Represents the current comparison result.
/// </summary>
/// <remarks>
/// Comparison results are immutable snapshots. Row and summary collections are
/// materialized so consumers can inspect the result deterministically without
/// re-running comparison logic. Rows keep stable row positions and source
/// record IDs so a result can be exported, explained, and reviewed without a
/// debugger.
/// </remarks>
public sealed class ComparisonResult
{
    /// <summary>
    /// Creates a comparison result.
    /// </summary>
    /// <param name="plan">The comparison plan.</param>
    /// <param name="diagnostics">The validation and execution diagnostics.</param>
    /// <param name="prepared">The prepared comparison artifact, when execution reached preparation.</param>
    /// <param name="aligned">The aligned comparison artifact, when execution reached alignment.</param>
    /// <param name="comparatorSummaries">The comparator summaries.</param>
    /// <param name="overlapRows">Rows emitted by the overlap comparator.</param>
    /// <param name="residualRows">Rows emitted by the residual comparator.</param>
    /// <param name="missingRows">Rows emitted by the missing comparator.</param>
    /// <param name="coverageRows">Rows emitted by the coverage comparator.</param>
    /// <param name="coverageSummaries">Summaries emitted by the coverage comparator.</param>
    public ComparisonResult(
        ComparisonPlan plan,
        IEnumerable<ComparisonPlanDiagnostic> diagnostics,
        PreparedComparison? prepared = null,
        AlignedComparison? aligned = null,
        IEnumerable<ComparatorSummary>? comparatorSummaries = null,
        IEnumerable<OverlapRow>? overlapRows = null,
        IEnumerable<ResidualRow>? residualRows = null,
        IEnumerable<MissingRow>? missingRows = null,
        IEnumerable<CoverageRow>? coverageRows = null,
        IEnumerable<CoverageSummary>? coverageSummaries = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(diagnostics);

        Plan = plan;
        Diagnostics = Materialize(diagnostics);
        Prepared = prepared;
        Aligned = aligned;
        ComparatorSummaries = Materialize(comparatorSummaries);
        OverlapRows = Materialize(overlapRows);
        ResidualRows = Materialize(residualRows);
        MissingRows = Materialize(missingRows);
        CoverageRows = Materialize(coverageRows);
        CoverageSummaries = Materialize(coverageSummaries);
    }

    /// <summary>
    /// Gets the comparison plan.
    /// </summary>
    public ComparisonPlan Plan { get; }

    /// <summary>
    /// Gets the validation and execution diagnostics.
    /// </summary>
    public IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics { get; }

    /// <summary>
    /// Gets the prepared comparison artifact, when execution reached preparation.
    /// </summary>
    public PreparedComparison? Prepared { get; }

    /// <summary>
    /// Gets the aligned comparison artifact, when execution reached alignment.
    /// </summary>
    public AlignedComparison? Aligned { get; }

    /// <summary>
    /// Gets comparator summaries in declaration order.
    /// </summary>
    public IReadOnlyList<ComparatorSummary> ComparatorSummaries { get; }

    /// <summary>
    /// Gets rows emitted by the overlap comparator.
    /// </summary>
    public IReadOnlyList<OverlapRow> OverlapRows { get; }

    /// <summary>
    /// Gets rows emitted by the residual comparator.
    /// </summary>
    public IReadOnlyList<ResidualRow> ResidualRows { get; }

    /// <summary>
    /// Gets rows emitted by the missing comparator.
    /// </summary>
    public IReadOnlyList<MissingRow> MissingRows { get; }

    /// <summary>
    /// Gets rows emitted by the coverage comparator.
    /// </summary>
    public IReadOnlyList<CoverageRow> CoverageRows { get; }

    /// <summary>
    /// Gets summaries emitted by the coverage comparator.
    /// </summary>
    public IReadOnlyList<CoverageSummary> CoverageSummaries { get; }

    /// <summary>
    /// Gets whether the result has no error diagnostics.
    /// </summary>
    /// <remarks>
    /// Warnings remain visible in <see cref="Diagnostics" /> and explain/export
    /// output, but they do not make the result invalid.
    /// </remarks>
    public bool IsValid => Diagnostics.All(static diagnostic =>
        diagnostic.Severity != ComparisonPlanDiagnosticSeverity.Error);

    private static IReadOnlyList<T> Materialize<T>(IEnumerable<T>? values)
    {
        return values switch
        {
            null => [],
            T[] array => array,
            _ => values.ToArray()
        };
    }
}
