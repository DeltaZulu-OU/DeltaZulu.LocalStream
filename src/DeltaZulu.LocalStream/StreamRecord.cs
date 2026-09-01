namespace DeltaZulu.LocalStream;

/// <summary>
/// A record read from a topic partition. <see cref="EventId"/> is the stable
/// cross-system identity; <see cref="Topic"/>, <see cref="Partition"/>, and
/// <see cref="Offset"/> are the stream coordinates. Sinks should use one of
/// them for idempotency.
/// </summary>
public sealed record StreamRecord<T>
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string EventId { get; init; }
    public required DateTimeOffset PublishedUtc { get; init; }
    /// <summary>Durable logical key, when supplied by the producer.</summary>
    public string? Key { get; init; }
    /// <summary>
    /// Durable event timestamp, when supplied by the producer. A missing value
    /// remains missing and is not replaced with <see cref="PublishedUtc"/>.
    /// </summary>
    public DateTimeOffset? EventTimeUtc { get; init; }
    public required IReadOnlyDictionary<string, string> Headers { get; init; }
    public required T Payload { get; init; }

    /// <summary>The stream coordinates of this record, suitable for <c>CommitAsync</c>.</summary>
    public StreamPosition Position => new(Topic, Partition, Offset);
}
