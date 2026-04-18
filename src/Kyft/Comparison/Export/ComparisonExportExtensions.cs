using Kyft.Internal.Comparison;

namespace Kyft;

/// <summary>
/// Provides deterministic export helpers for comparison artifacts.
/// </summary>
/// <remarks>
/// JSON export uses a stable schema envelope and deterministic property order.
/// It is intended for CI artifacts, issue reports, notebooks, and agent
/// consumption rather than hot-path analytics execution.
/// </remarks>
public static class ComparisonExportExtensions
{
    /// <summary>
    /// Exports a comparison plan as deterministic JSON.
    /// </summary>
    /// <param name="plan">The plan to export.</param>
    /// <returns>Deterministic JSON for the plan.</returns>
    /// <exception cref="ComparisonExportException">
    /// Thrown when the plan contains runtime-only selectors that cannot be
    /// exported as portable data.
    /// </exception>
    public static string ExportJson(this ComparisonPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        return ComparisonExporter.ExportJson(plan);
    }

    /// <summary>
    /// Exports a comparison result as deterministic JSON.
    /// </summary>
    /// <param name="result">The result to export.</param>
    /// <returns>Deterministic JSON for the result.</returns>
    /// <exception cref="ComparisonExportException">
    /// Thrown when the result's plan contains runtime-only selectors that
    /// cannot be exported as portable data.
    /// </exception>
    public static string ExportJson(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return ComparisonExporter.ExportJson(result);
    }

    /// <summary>
    /// Exports a comparison plan as deterministic Markdown.
    /// </summary>
    /// <param name="plan">The plan to export.</param>
    /// <returns>Deterministic Markdown for the plan.</returns>
    public static string ExportMarkdown(this ComparisonPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        return plan.Explain(ComparisonExplanationFormat.Markdown);
    }

    /// <summary>
    /// Exports a comparison result as deterministic Markdown.
    /// </summary>
    /// <param name="result">The result to export.</param>
    /// <returns>Deterministic Markdown for the result.</returns>
    public static string ExportMarkdown(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Explain(ComparisonExplanationFormat.Markdown);
    }

    /// <summary>
    /// Exports result rows as deterministic JSON Lines.
    /// </summary>
    /// <param name="result">The result to export.</param>
    /// <returns>A lazy sequence of JSON Lines documents for result rows.</returns>
    /// <exception cref="ComparisonExportException">
    /// Thrown when the result's plan contains runtime-only selectors that
    /// cannot be exported as portable data.
    /// </exception>
    public static IEnumerable<string> ExportJsonLines(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return ComparisonExporter.ExportJsonLines(result);
    }
}
