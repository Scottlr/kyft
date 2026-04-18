namespace Kyft;

/// <summary>
/// Describes the common shape of an open or closed span.
/// </summary>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical key for the window.</param>
/// <param name="StartPosition">The processing position where the window started.</param>
/// <param name="EndPosition">The processing position where the window ended, if closed.</param>
/// <param name="Source">Optional source identity supplied when the window started.</param>
/// <param name="Partition">Optional partition identity supplied when the window started.</param>
/// <param name="StartTime">Optional event timestamp where the window started.</param>
/// <param name="EndTime">Optional event timestamp where the window ended, if closed.</param>
public abstract record WindowRecord(
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
    /// Gets the deterministic identity for this recorded window.
    /// </summary>
    /// <remarks>
    /// The identity is stable for the same recorded window data in a
    /// deterministic replay. It is not a distributed global identifier.
    /// </remarks>
    public WindowRecordId Id => WindowRecordId.From(this);

    /// <summary>
    /// Gets whether this window has an end position.
    /// </summary>
    public bool IsClosed => EndPosition.HasValue;
}
