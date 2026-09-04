namespace DeltaZulu.LocalStream;

public interface ILocalStreamConsumer<T>
{
    string SubscriptionId { get; }

    /// <summary>
    /// Reads a topic's partitions in order, partition 0 first through the
    /// last, each drained to completion before the next starts — there is no
    /// interleaving by timestamp or offset across partitions. Enumeration
    /// performs blocking synchronous file I/O on the calling thread; the
    /// method is <c>async</c> only to satisfy <see cref="IAsyncEnumerable{T}"/>,
    /// and does not yield control back to the caller between records. Because
    /// each partition's start offset is resolved lazily as the enumeration
    /// reaches it, an <see cref="OffsetExpiredException"/> for a later
    /// partition can surface only after records from earlier partitions have
    /// already been yielded.
    /// </summary>
    IAsyncEnumerable<StreamRecord<T>> ReadAsync(
        string topic,
        ReadOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask CommitAsync(
        StreamPosition position,
        CancellationToken cancellationToken = default);

    ValueTask ResetAsync(
        string topic,
        ResetPosition position,
        CancellationToken cancellationToken = default);
}
