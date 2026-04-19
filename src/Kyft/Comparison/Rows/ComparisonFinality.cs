namespace Kyft;

/// <summary>
/// Describes whether a comparison row is final or depends on an open window.
/// </summary>
public enum ComparisonFinality
{
    /// <summary>
    /// The row was produced only from closed windows.
    /// </summary>
    Final = 0,

    /// <summary>
    /// The row depends on at least one open window clipped to an evaluation horizon.
    /// </summary>
    Provisional = 1,

    /// <summary>
    /// The row supersedes an earlier emitted row version.
    /// </summary>
    Revised = 2,

    /// <summary>
    /// The row was removed by a later snapshot.
    /// </summary>
    Retracted = 3
}
