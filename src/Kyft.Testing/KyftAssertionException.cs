namespace Kyft.Testing;

/// <summary>
/// Represents an assertion failure produced by Kyft testing helpers.
/// </summary>
public sealed class KyftAssertionException : Exception
{
    /// <summary>
    /// Creates an assertion exception.
    /// </summary>
    /// <param name="message">The assertion failure message.</param>
    public KyftAssertionException(string message)
        : base(message)
    {
    }
}
