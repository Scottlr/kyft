namespace Kyft;

/// <summary>
/// Describes a currently open recorded chunk.
/// </summary>
public sealed record OpenChunk : Chunk
{
    /// <summary>
    /// Creates a currently open recorded chunk.
    /// </summary>
    /// <param name="WindowName">The configured window name.</param>
    /// <param name="Key">The logical key for the window.</param>
    /// <param name="StartPosition">The processing position where the chunk opened.</param>
    /// <param name="Source">Optional source identity supplied when the chunk opened.</param>
    /// <param name="Partition">Optional partition identity supplied when the chunk opened.</param>
    /// <param name="StartTime">Optional event timestamp where the chunk opened.</param>
    public OpenChunk(
        string WindowName,
        object Key,
        long StartPosition,
        object? Source = null,
        object? Partition = null,
        DateTimeOffset? StartTime = null)
        : base(
        WindowName,
        Key,
        StartPosition,
        EndPosition: null,
        Source,
        Partition,
        StartTime,
        EndTime: null)
    {
    }
}
