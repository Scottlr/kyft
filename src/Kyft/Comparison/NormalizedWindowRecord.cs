namespace Kyft;

/// <summary>
/// Describes a recorded window after comparison normalization.
/// </summary>
/// <param name="Window">The source recorded window.</param>
/// <param name="RecordId">The source window identifier.</param>
/// <param name="SelectorName">The selector that matched the window.</param>
/// <param name="Side">The comparison side.</param>
/// <param name="Range">The normalized temporal range.</param>
public sealed record NormalizedWindowRecord(
    WindowRecord Window,
    WindowRecordId RecordId,
    string SelectorName,
    ComparisonSide Side,
    TemporalRange Range);
