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
public sealed record ComparisonResult(
    ComparisonPlan Plan,
    IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics)
{
    /// <summary>
    /// Gets whether the plan produced no validation diagnostics.
    /// </summary>
    public bool IsValid => Diagnostics.All(static diagnostic =>
        diagnostic.Severity != ComparisonPlanDiagnosticSeverity.Error);
}
