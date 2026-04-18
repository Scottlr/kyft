namespace Kyft;

/// <summary>
/// Identifies a validation diagnostic produced by a comparison plan.
/// </summary>
public enum ComparisonPlanValidationCode
{
    /// <summary>
    /// Indicates an unspecified validation issue.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Indicates that the plan name is missing.
    /// </summary>
    MissingName = 1,

    /// <summary>
    /// Indicates that the target selector is missing.
    /// </summary>
    MissingTarget = 2,

    /// <summary>
    /// Indicates that the plan has no comparison selectors.
    /// </summary>
    MissingAgainst = 3,

    /// <summary>
    /// Indicates that the plan has no comparator declarations.
    /// </summary>
    MissingComparator = 4,

    /// <summary>
    /// Indicates that a selector cannot be exported as a plan descriptor.
    /// </summary>
    NonSerializableSelector = 5,

    /// <summary>
    /// Indicates that the plan has no temporal scope.
    /// </summary>
    MissingScope = 6
}
