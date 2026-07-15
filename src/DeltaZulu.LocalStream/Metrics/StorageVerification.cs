namespace DeltaZulu.LocalStream;

/// <summary>Read-only integrity scan result for one partition.</summary>
public sealed record PartitionVerification(
    int Partition,
    int SegmentsScanned,
    long ValidRecords,
    long TrailingGarbageBytes);

/// <summary>
/// Read-only integrity report for one topic: every segment line is re-framed
/// and CRC-validated. <see cref="IsClean"/> is false when any segment carries
/// bytes past its last valid record (torn tail or corruption). Verification
/// never modifies files; startup recovery performs the actual truncation.
/// </summary>
public sealed record StorageVerification(
    string Topic,
    int SegmentsScanned,
    long ValidRecords,
    bool IsClean,
    IReadOnlyList<PartitionVerification> Partitions);
