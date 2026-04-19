namespace Kyft;

/// <summary>
/// Represents a directional pairwise matrix over recorded window sources.
/// </summary>
/// <param name="Name">The matrix name.</param>
/// <param name="WindowName">The recorded window name used for every cell.</param>
/// <param name="Sources">The sources in requested row and column order.</param>
/// <param name="Cells">The directional matrix cells in row-major source order.</param>
public sealed record SourceMatrixResult(
    string Name,
    string WindowName,
    IReadOnlyList<object> Sources,
    IReadOnlyList<SourceMatrixCell> Cells);
