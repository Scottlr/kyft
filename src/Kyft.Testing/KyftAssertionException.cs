namespace Kyft.Testing;

/// <summary>
/// Represents an assertion failure produced by Kyft testing helpers.
/// </summary>
/// <param name="message">The assertion failure message.</param>
public sealed class KyftAssertionException(string message) : Exception(message)
{
}
