namespace Spanfold;

/// <summary>
/// Describes why a recorded window boundary was emitted.
/// </summary>
public enum WindowBoundaryReason
{
    /// <summary>
    /// The active predicate changed from true to false.
    /// </summary>
    ActivePredicateEnded = 0,

    /// <summary>
    /// The window remained active but one or more segment values changed.
    /// </summary>
    SegmentChanged = 1
}
