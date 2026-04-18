using Kyft.Internal.Comparison;

namespace Kyft;

/// <summary>
/// Represents the current prepared comparison state.
/// </summary>
/// <remarks>
/// This artifact carries the plan, validation diagnostics, selected windows,
/// excluded windows, and normalized windows ready for alignment.
/// </remarks>
/// <param name="Plan">The comparison plan.</param>
/// <param name="Diagnostics">The validation diagnostics.</param>
/// <param name="SelectedWindows">The recorded windows selected by the plan.</param>
/// <param name="ExcludedWindows">The recorded windows excluded during preparation.</param>
/// <param name="NormalizedWindows">The normalized windows ready for alignment.</param>
public sealed record PreparedComparison(
    ComparisonPlan Plan,
    IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics,
    IReadOnlyList<WindowRecord> SelectedWindows,
    IReadOnlyList<ExcludedWindowRecord> ExcludedWindows,
    IReadOnlyList<NormalizedWindowRecord> NormalizedWindows)
{
    /// <summary>
    /// Aligns normalized windows into reusable temporal segments.
    /// </summary>
    /// <returns>The aligned comparison.</returns>
    public AlignedComparison Align()
    {
        return ComparisonAligner.Align(this);
    }
}
