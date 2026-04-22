namespace Spanfold;

/// <summary>
/// Configures optional LLM context export for comparison runs.
/// </summary>
/// <remarks>
/// Use this when application configuration controls whether historical or live
/// comparison runs should emit an agent-readable context artifact.
/// </remarks>
public sealed record ComparisonLlmContextOptions
{
    private ComparisonLlmContextOptions(bool enabled, string? path)
    {
        if (enabled)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
        }

        Enabled = enabled;
        Path = path;
    }

    /// <summary>
    /// Gets a value indicating whether LLM context export is enabled.
    /// </summary>
    public bool Enabled { get; }

    /// <summary>
    /// Gets the destination file path when export is enabled.
    /// </summary>
    public string? Path { get; }

    /// <summary>
    /// Gets an option value that disables LLM context export.
    /// </summary>
    public static ComparisonLlmContextOptions Disabled { get; } = new(
        enabled: false,
        path: null);

    /// <summary>
    /// Creates an option value that writes deterministic LLM context JSON to a file.
    /// </summary>
    /// <param name="path">The destination JSON file path.</param>
    /// <returns>An enabled LLM context export option.</returns>
    public static ComparisonLlmContextOptions ToFile(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        return new ComparisonLlmContextOptions(
            enabled: true,
            path: path);
    }

    internal void ExportIfEnabled(ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (!Enabled)
        {
            return;
        }

        result.ExportLlmContext(Path!);
    }
}
