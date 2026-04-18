namespace Kyft;

/// <summary>
/// Describes one aligned temporal segment and the normalized windows active within it.
/// </summary>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical window key.</param>
/// <param name="Partition">The optional partition identity.</param>
/// <param name="Range">The aligned segment range.</param>
/// <param name="TargetRecordIds">The target window IDs active for the segment.</param>
/// <param name="AgainstRecordIds">The comparison window IDs active for the segment.</param>
public sealed record AlignedSegment(
    string WindowName,
    object Key,
    object? Partition,
    TemporalRange Range,
    IReadOnlyList<WindowRecordId> TargetRecordIds,
    IReadOnlyList<WindowRecordId> AgainstRecordIds);
