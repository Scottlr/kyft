namespace Kyft;

/// <summary>
/// Represents the current prepared comparison state.
/// </summary>
/// <remarks>
/// This initial artifact carries the plan and validation diagnostics. Later
/// normalization specs will add selected, excluded, and normalized windows.
/// </remarks>
/// <param name="Plan">The comparison plan.</param>
/// <param name="Diagnostics">The validation diagnostics.</param>
public sealed record PreparedComparison(
    ComparisonPlan Plan,
    IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics);
