namespace Spanfold.Tests.Support;

[Flags]
internal enum SnapshotNormalization
{
    None = 0,
    RecordIds = 1,
    Default = RecordIds
}
