namespace Kyft.Internal.Recording;

internal sealed record WindowRecordingKey(
    string WindowName,
    object Key,
    object? Source,
    object? Partition);
