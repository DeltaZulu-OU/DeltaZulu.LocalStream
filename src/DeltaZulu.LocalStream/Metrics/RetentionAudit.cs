namespace DeltaZulu.LocalStream;

/// <summary>What retention would delete right now in one partition.</summary>
public sealed record PartitionRetentionAudit(
    int Partition,
    int DeletableSegments,
    long DeletableBytes,
    long DeletableRecords,
    long FirstRetainedOffsetAfterDeletion);

/// <summary>
/// Dry-run retention report for one topic: the sealed segments the current
/// policy would delete, without deleting anything.
/// </summary>
public sealed record RetentionAudit(
    string Topic,
    int DeletableSegments,
    long DeletableBytes,
    long DeletableRecords,
    IReadOnlyList<PartitionRetentionAudit> Partitions);
