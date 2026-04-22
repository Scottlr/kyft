namespace Spanfold;

/// <summary>
/// Describes a target-source window segment left after comparison overlap is removed.
/// </summary>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical key for the window.</param>
/// <param name="Source">The target source identity.</param>
/// <param name="StartPosition">The residual segment start position.</param>
/// <param name="EndPosition">The residual segment end position.</param>
/// <param name="Partition">Optional partition identity for the residual segment.</param>
public sealed record WindowResidualSegment(
    string WindowName,
    object Key,
    object Source,
    long StartPosition,
    long EndPosition,
    object? Partition = null);
