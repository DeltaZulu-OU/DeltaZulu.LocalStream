namespace DeltaZulu.LocalStream;

public enum ReadStart
{
    /// <summary>
    /// Resume after the subscription's committed offset. A subscription without
    /// a checkpoint starts from the earliest retained record.
    /// </summary>
    Committed,

    /// <summary>Read from the earliest retained record, ignoring the checkpoint.</summary>
    Earliest,

    /// <summary>Read only records appended after the current end of the partition.</summary>
    Latest,
}

public sealed class ReadOptions
{
    public ReadStart Start { get; init; } = ReadStart.Committed;

    /// <summary>Restrict the read to a single partition. All partitions when null.</summary>
    public int? Partition { get; init; }
}
