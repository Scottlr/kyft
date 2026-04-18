namespace Kyft;

/// <summary>
/// Describes output preferences for a comparison plan.
/// </summary>
/// <param name="IncludeAlignedSegments">Whether result output should include aligned segment details.</param>
/// <param name="IncludeExplainData">Whether result output should include explain data.</param>
public sealed record ComparisonOutputOptions(
    bool IncludeAlignedSegments,
    bool IncludeExplainData)
{
    /// <summary>
    /// Gets the default comparison output options.
    /// </summary>
    public static ComparisonOutputOptions Default { get; } = new(
        IncludeAlignedSegments: true,
        IncludeExplainData: true);
}
