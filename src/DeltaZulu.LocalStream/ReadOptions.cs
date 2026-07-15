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

    /// <summary>Replay from <see cref="ReadOptions.Offset"/>, ignoring the checkpoint.</summary>
    Offset,

    /// <summary>
    /// Replay from the first record published at or after
    /// <see cref="ReadOptions.Timestamp"/>, ignoring the checkpoint.
    /// </summary>
    Timestamp,
}

public sealed class ReadOptions
{
    public ReadStart Start { get; init; } = ReadStart.Committed;

    /// <summary>Restrict the read to a single partition. All partitions when null.</summary>
    public int? Partition { get; init; }

    /// <summary>Replay start offset; only used with <see cref="ReadStart.Offset"/>.</summary>
    public long Offset { get; init; }

    /// <summary>Replay start timestamp; only used with <see cref="ReadStart.Timestamp"/>.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>A one-off replay read from an explicit offset. Never moves the checkpoint.</summary>
    public static ReadOptions FromOffset(long offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return new ReadOptions { Start = ReadStart.Offset, Offset = offset };
    }

    /// <summary>A one-off replay read from a publish timestamp. Never moves the checkpoint.</summary>
    public static ReadOptions FromTimestamp(DateTimeOffset timestamp) =>
        new() { Start = ReadStart.Timestamp, Timestamp = timestamp };
}
