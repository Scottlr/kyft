namespace Spanfold;

/// <summary>
/// Configures optional debug HTML export during comparison execution.
/// </summary>
/// <remarks>
/// Use these options when application configuration decides whether a
/// comparison run should emit a visual debug artifact. Disabled options keep
/// execution free of file output.
/// </remarks>
public sealed record ComparisonDebugHtmlOptions
{
    private ComparisonDebugHtmlOptions(bool enabled, string? path)
    {
        Enabled = enabled;
        Path = path;
    }

    /// <summary>
    /// Gets whether debug HTML export is enabled.
    /// </summary>
    public bool Enabled { get; }

    /// <summary>
    /// Gets the destination file path when export is enabled.
    /// </summary>
    public string? Path { get; }

    /// <summary>
    /// Gets options that do not write a debug HTML artifact.
    /// </summary>
    public static ComparisonDebugHtmlOptions Disabled { get; } = new(
        enabled: false,
        path: null);

    /// <summary>
    /// Creates options that write a debug HTML artifact to a file.
    /// </summary>
    /// <param name="path">The destination HTML file path.</param>
    /// <returns>Debug HTML options that write to <paramref name="path" />.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="path" /> is empty.</exception>
    public static ComparisonDebugHtmlOptions ToFile(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        return new ComparisonDebugHtmlOptions(
            enabled: true,
            path);
    }

    internal void ExportIfEnabled(ComparisonResult result)
    {
        if (!Enabled)
        {
            return;
        }

        result.ExportDebugHtml(Path!);
    }
}
