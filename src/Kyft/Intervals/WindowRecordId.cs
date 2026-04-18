using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Kyft;

/// <summary>
/// Identifies a recorded window occurrence in a deterministic replay.
/// </summary>
/// <remarks>
/// Window record IDs are stable for the same recorded window data within a
/// deterministic replay. They are not intended to be globally unique across
/// unrelated systems or independently produced histories.
/// </remarks>
/// <param name="Value">The deterministic identifier value.</param>
public readonly record struct WindowRecordId(string Value)
{
    /// <summary>
    /// Creates a deterministic identifier for a recorded window.
    /// </summary>
    /// <param name="window">The recorded window to identify.</param>
    /// <returns>A deterministic window record identifier.</returns>
    public static WindowRecordId From(WindowRecord window)
    {
        ArgumentNullException.ThrowIfNull(window);

        var builder = new StringBuilder(capacity: 256);
        Append(builder, "window", window.WindowName);
        Append(builder, "key", StableObjectValue(window.Key));
        Append(builder, "source", StableObjectValue(window.Source));
        Append(builder, "partition", StableObjectValue(window.Partition));
        Append(builder, "start-position", window.StartPosition.ToString(CultureInfo.InvariantCulture));
        Append(builder, "end-position", window.EndPosition?.ToString(CultureInfo.InvariantCulture) ?? "<open>");
        Append(builder, "start-time", StableTimestampValue(window.StartTime));
        Append(builder, "end-time", StableTimestampValue(window.EndTime));
        Append(builder, "end-status", window.IsClosed ? "closed" : "open");

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return new WindowRecordId(Convert.ToHexString(bytes).ToLowerInvariant());
    }

    /// <summary>
    /// Returns the identifier value.
    /// </summary>
    /// <returns>The identifier value.</returns>
    public override string ToString()
    {
        return Value;
    }

    private static void Append(StringBuilder builder, string name, string value)
    {
        builder
            .Append(name)
            .Append('=')
            .Append(value.Length.ToString(CultureInfo.InvariantCulture))
            .Append(':')
            .Append(value)
            .Append(';');
    }

    private static string StableObjectValue(object? value)
    {
        return value switch
        {
            null => "<null>",
            IFormattable formattable => value.GetType().FullName + ":" + formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.GetType().FullName + ":" + value
        };
    }

    private static string StableTimestampValue(DateTimeOffset? value)
    {
        return value?.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture) ?? "<null>";
    }
}
