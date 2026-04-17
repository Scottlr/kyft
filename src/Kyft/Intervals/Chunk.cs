namespace Kyft;

/// <summary>
/// Describes the common shape of an open or closed span.
/// </summary>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical key for the window.</param>
/// <param name="StartPosition">The processing position where the chunk started.</param>
/// <param name="EndPosition">The processing position where the chunk ended, if closed.</param>
/// <param name="Source">Optional source identity supplied when the chunk started.</param>
/// <param name="Partition">Optional partition identity supplied when the chunk started.</param>
/// <param name="StartTime">Optional event timestamp where the chunk started.</param>
/// <param name="EndTime">Optional event timestamp where the chunk ended, if closed.</param>
public abstract record Chunk(
    string WindowName,
    object Key,
    long StartPosition,
    long? EndPosition,
    object? Source = null,
    object? Partition = null,
    DateTimeOffset? StartTime = null,
    DateTimeOffset? EndTime = null)
{
    /// <summary>
    /// Gets whether this chunk has an end position.
    /// </summary>
    public bool IsClosed => EndPosition.HasValue;
}
