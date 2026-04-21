namespace Kyft;

/// <summary>
/// Identifies the metadata family used to group recorded windows.
/// </summary>
public enum WindowGroupKind
{
    /// <summary>
    /// The group was produced from analytical boundary segments.
    /// </summary>
    Segment,

    /// <summary>
    /// The group was produced from descriptive tags.
    /// </summary>
    Tag
}
