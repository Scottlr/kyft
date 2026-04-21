namespace Kyft;

/// <summary>
/// Identifies the stable start identity of a recorded window for annotation.
/// </summary>
/// <remarks>
/// The target intentionally excludes the window end so annotations attached to
/// an open window remain associated after that window closes.
/// </remarks>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical key for the window.</param>
/// <param name="StartPosition">The processing position where the window started.</param>
/// <param name="Source">Optional source identity supplied when the window started.</param>
/// <param name="Partition">Optional partition identity supplied when the window started.</param>
/// <param name="StartTime">Optional event timestamp where the window started.</param>
public sealed record WindowAnnotationTarget(
    string WindowName,
    object Key,
    long StartPosition,
    object? Source = null,
    object? Partition = null,
    DateTimeOffset? StartTime = null)
{
    /// <summary>
    /// Creates an annotation target from a recorded window.
    /// </summary>
    /// <param name="window">The recorded window.</param>
    /// <returns>The stable annotation target for the window.</returns>
    public static WindowAnnotationTarget From(WindowRecord window)
    {
        ArgumentNullException.ThrowIfNull(window);

        return new WindowAnnotationTarget(
            window.WindowName,
            window.Key,
            window.StartPosition,
            window.Source,
            window.Partition,
            window.StartTime);
    }
}
