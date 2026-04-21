namespace Spanfold.Testing;

/// <summary>
/// Represents an assertion failure produced by Spanfold testing helpers.
/// </summary>
/// <param name="message">The assertion failure message.</param>
public sealed class SpanfoldAssertionException(string message) : Exception(message)
{
}
