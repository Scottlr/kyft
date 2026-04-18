namespace Kyft;

/// <summary>
/// Identifies the temporal axis used to order a point in a Kyft analysis.
/// </summary>
public enum TemporalAxis
{
    /// <summary>
    /// Indicates that no temporal axis has been selected.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Orders points by the pipeline processing position assigned during ingestion.
    /// </summary>
    ProcessingPosition = 1,

    /// <summary>
    /// Orders points by an event timestamp.
    /// </summary>
    Timestamp = 2
}
