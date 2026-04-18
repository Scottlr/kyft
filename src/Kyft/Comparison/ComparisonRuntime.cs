namespace Kyft;

internal static class ComparisonRuntime
{
    private static readonly HashSet<string> KnownComparators = new(StringComparer.Ordinal)
    {
        "overlap",
        "residual",
        "missing",
        "coverage"
    };

    internal static ComparisonResult Run(PreparedComparison prepared)
    {
        var aligned = prepared.Align();
        var diagnostics = new List<ComparisonPlanDiagnostic>(prepared.Diagnostics);
        var summaries = new List<ComparatorSummary>();

        for (var i = 0; i < prepared.Plan.Comparators.Count; i++)
        {
            var comparator = prepared.Plan.Comparators[i];
            if (!KnownComparators.Contains(comparator))
            {
                diagnostics.Add(new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.UnknownComparator,
                    $"Comparator '{comparator}' is not registered.",
                    $"comparators[{i}]",
                    ComparisonPlanDiagnosticSeverity.Error));
                continue;
            }

            summaries.Add(new ComparatorSummary(comparator, RowCount: 0));
        }

        return new ComparisonResult(
            prepared.Plan,
            diagnostics.ToArray(),
            prepared,
            aligned,
            summaries.ToArray());
    }
}
