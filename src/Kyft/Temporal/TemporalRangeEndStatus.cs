namespace Kyft;

/// <summary>
/// Describes how the end of a temporal range was determined.
/// </summary>
public enum TemporalRangeEndStatus
{
    /// <summary>
    /// Indicates that no end status has been selected.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Indicates that the range ended because the recorded window closed.
    /// </summary>
    Closed = 1,

    /// <summary>
    /// Indicates that the range is still open and has no effective end.
    /// </summary>
    UnknownEnd = 2,

    /// <summary>
    /// Indicates that an open range was clipped to an evaluation horizon.
    /// </summary>
    OpenAtHorizon = 3,

    /// <summary>
    /// Indicates that the range was clipped by the query range.
    /// </summary>
    ClippedByQueryRange = 4
}
