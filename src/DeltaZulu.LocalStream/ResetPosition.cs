namespace DeltaZulu.LocalStream;

public enum ResetKind
{
    Earliest,
    Latest,
    Offset,
    Timestamp,
}

/// <summary>Target position for an operator-driven subscription reset/replay.</summary>
public sealed record ResetPosition
{
    private ResetPosition(ResetKind kind, long offset, DateTimeOffset timestamp)
    {
        Kind = kind;
        Offset = offset;
        Timestamp = timestamp;
    }

    public ResetKind Kind { get; }
    public long Offset { get; }
    public DateTimeOffset Timestamp { get; }

    public static ResetPosition Earliest() => new(ResetKind.Earliest, 0, default);

    public static ResetPosition Latest() => new(ResetKind.Latest, 0, default);

    public static ResetPosition AtOffset(long offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return new(ResetKind.Offset, offset, default);
    }

    public static ResetPosition AtTimestamp(DateTimeOffset timestamp) =>
        new(ResetKind.Timestamp, 0, timestamp);
}
