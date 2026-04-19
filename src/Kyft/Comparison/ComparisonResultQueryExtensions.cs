namespace Kyft;

/// <summary>
/// Provides small query helpers over materialized comparison results.
/// </summary>
public static class ComparisonResultQueryExtensions
{
    /// <summary>
    /// Gets error diagnostics from a comparison result.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>Error diagnostics in result order.</returns>
    public static IReadOnlyList<ComparisonPlanDiagnostic> ErrorDiagnostics(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return DiagnosticsBySeverity(result, ComparisonPlanDiagnosticSeverity.Error);
    }

    /// <summary>
    /// Gets warning diagnostics from a comparison result.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>Warning diagnostics in result order.</returns>
    public static IReadOnlyList<ComparisonPlanDiagnostic> WarningDiagnostics(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return DiagnosticsBySeverity(result, ComparisonPlanDiagnosticSeverity.Warning);
    }

    /// <summary>
    /// Gets provisional row finality metadata from a comparison result.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>Provisional row finality metadata in result order.</returns>
    public static IReadOnlyList<ComparisonRowFinality> ProvisionalRowFinalities(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return FinalitiesByState(result, ComparisonFinality.Provisional);
    }

    /// <summary>
    /// Gets final row finality metadata from a comparison result.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>Final row finality metadata in result order.</returns>
    public static IReadOnlyList<ComparisonRowFinality> FinalRowFinalities(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return FinalitiesByState(result, ComparisonFinality.Final);
    }

    /// <summary>
    /// Gets whether the result contains any provisional rows.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>True when at least one row is provisional.</returns>
    public static bool HasProvisionalRows(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        for (var i = 0; i < result.RowFinalities.Count; i++)
        {
            if (result.RowFinalities[i].Finality == ComparisonFinality.Provisional)
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<ComparisonPlanDiagnostic> DiagnosticsBySeverity(
        ComparisonResult result,
        ComparisonPlanDiagnosticSeverity severity)
    {
        var diagnostics = new List<ComparisonPlanDiagnostic>();
        for (var i = 0; i < result.Diagnostics.Count; i++)
        {
            var diagnostic = result.Diagnostics[i];
            if (diagnostic.Severity == severity)
            {
                diagnostics.Add(diagnostic);
            }
        }

        return diagnostics.ToArray();
    }

    private static IReadOnlyList<ComparisonRowFinality> FinalitiesByState(
        ComparisonResult result,
        ComparisonFinality finality)
    {
        var finalities = new List<ComparisonRowFinality>();
        for (var i = 0; i < result.RowFinalities.Count; i++)
        {
            var rowFinality = result.RowFinalities[i];
            if (rowFinality.Finality == finality)
            {
                finalities.Add(rowFinality);
            }
        }

        return finalities.ToArray();
    }
}
