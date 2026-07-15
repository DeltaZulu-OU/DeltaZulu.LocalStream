namespace DeltaZulu.LocalStream;

public enum AppendStatus
{
    Appended,
    RejectedTopicNotFound,
    RejectedStreamFull,
    RejectedRecordTooLarge,
    FailedSerialization,
    FailedStorage,
    Cancelled,
}

public sealed record AppendResult
{
    public required AppendStatus Status { get; init; }
    public StreamPosition? Position { get; init; }
    public string? EventId { get; init; }
    public string? Reason { get; init; }
}
