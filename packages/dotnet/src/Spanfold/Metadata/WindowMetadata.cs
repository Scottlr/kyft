namespace Spanfold;

/// <summary>
/// Describes a configured window or roll-up.
/// </summary>
/// <param name="Name">The configured window name.</param>
/// <param name="RollUps">The child roll-ups attached to this window.</param>
public sealed record WindowMetadata(
    string Name,
    IReadOnlyList<WindowMetadata> RollUps);
