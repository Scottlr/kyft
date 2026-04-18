namespace Kyft;

/// <summary>
/// Describes the temporal scope for a comparison plan.
/// </summary>
/// <remarks>
/// Scope selects the window family and temporal axis for the question. The
/// normalization policy must use the same axis before preparation can produce
/// comparable ranges.
/// </remarks>
/// <param name="WindowName">Optional window name restriction.</param>
/// <param name="TimeAxis">The temporal axis used by the comparison.</param>
public sealed record ComparisonScope(
    string? WindowName,
    TemporalAxis TimeAxis)
{
    /// <summary>
    /// Creates an unrestricted scope on the processing-position axis.
    /// </summary>
    /// <returns>A comparison scope for all recorded windows.</returns>
    public static ComparisonScope All()
    {
        return new ComparisonScope(WindowName: null, TemporalAxis.ProcessingPosition);
    }

    /// <summary>
    /// Creates a scope restricted to one window name.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="timeAxis">The temporal axis used by the comparison.</param>
    /// <returns>A comparison scope for one window.</returns>
    public static ComparisonScope Window(string windowName, TemporalAxis timeAxis = TemporalAxis.ProcessingPosition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(windowName);

        return new ComparisonScope(windowName, timeAxis);
    }
}
