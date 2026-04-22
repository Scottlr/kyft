namespace Spanfold;

/// <summary>
/// Describes how duplicate normalized windows should be handled.
/// </summary>
public enum ComparisonDuplicateWindowPolicy
{
    /// <summary>
    /// Preserve duplicate windows and let diagnostics describe them.
    /// </summary>
    Preserve = 0,

    /// <summary>
    /// Reject duplicate normalized windows.
    /// </summary>
    Reject = 1
}
