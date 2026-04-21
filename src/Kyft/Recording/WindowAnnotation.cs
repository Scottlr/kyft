namespace Kyft;

/// <summary>
/// Describes external metadata attached to a recorded window.
/// </summary>
/// <remarks>
/// An annotation does not mutate, split, or revise the source window. Consumers
/// can append later revisions under the same name and decide how to interpret
/// them in their own read model.
/// </remarks>
/// <param name="Target">The stable window start identity being annotated.</param>
/// <param name="Name">The annotation name.</param>
/// <param name="Value">The annotation value.</param>
/// <param name="KnownAt">When the annotation became known, if supplied.</param>
/// <param name="Revision">The one-based revision number for this target and name.</param>
public sealed record WindowAnnotation(
    WindowAnnotationTarget Target,
    string Name,
    object? Value,
    TemporalPoint? KnownAt,
    int Revision);
