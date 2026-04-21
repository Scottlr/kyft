namespace Spanfold;

/// <summary>
/// Summarizes recorded windows that share one segment or tag value.
/// </summary>
/// <param name="GroupKind">The metadata family used to create the group.</param>
/// <param name="Name">The segment or tag name.</param>
/// <param name="Value">The segment or tag value.</param>
/// <param name="RecordCount">The number of records in the group.</param>
/// <param name="FinalCount">The number of records that are final for the evaluated history.</param>
/// <param name="ProvisionalCount">The number of records that may still change.</param>
/// <param name="MeasuredPositionCount">The number of records contributing processing-position length.</param>
/// <param name="TotalPositionLength">The total measured processing-position length.</param>
/// <param name="MeasuredTimeCount">The number of records contributing event-time duration.</param>
/// <param name="TotalTimeDuration">The total measured event-time duration.</param>
public sealed record WindowGroupSummary(
    WindowGroupKind GroupKind,
    string Name,
    object? Value,
    int RecordCount,
    int FinalCount,
    int ProvisionalCount,
    int MeasuredPositionCount,
    long TotalPositionLength,
    int MeasuredTimeCount,
    TimeSpan TotalTimeDuration);
