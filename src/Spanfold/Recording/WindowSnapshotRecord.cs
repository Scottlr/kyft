namespace Spanfold;

/// <summary>
/// Describes one recorded window as evaluated at a snapshot horizon.
/// </summary>
/// <remarks>
/// Snapshot records preserve the source window and add the range that is valid
/// for the requested horizon. A source window that is still active at the
/// horizon is clipped to that horizon and marked provisional.
/// </remarks>
/// <param name="Window">The source recorded window.</param>
/// <param name="Range">The range visible in the snapshot.</param>
/// <param name="Finality">Whether the snapshot record is final or provisional at the horizon.</param>
public sealed record WindowSnapshotRecord(
    WindowRecord Window,
    TemporalRange Range,
    ComparisonFinality Finality);
