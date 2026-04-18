namespace Kyft;

/// <summary>
/// Represents a comparison export failure with stable diagnostics.
/// </summary>
/// <remarks>
/// Export failures are deterministic and usually indicate that an artifact
/// contains runtime-only selectors or other data that cannot be represented as
/// portable JSON.
/// </remarks>
public sealed class ComparisonExportException : InvalidOperationException
{
    /// <summary>
    /// Creates an export exception.
    /// </summary>
    /// <param name="message">The readable failure message.</param>
    /// <param name="diagnostics">The diagnostics that explain why export failed.</param>
    public ComparisonExportException(
        string message,
        IEnumerable<ComparisonPlanDiagnostic> diagnostics)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(diagnostics);

        Diagnostics = diagnostics as ComparisonPlanDiagnostic[] ?? diagnostics.ToArray();
    }

    /// <summary>
    /// Gets the stable diagnostics that explain why export failed.
    /// </summary>
    public IReadOnlyList<ComparisonPlanDiagnostic> Diagnostics { get; }
}
