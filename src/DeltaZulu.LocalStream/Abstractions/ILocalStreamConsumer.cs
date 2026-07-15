namespace DeltaZulu.LocalStream;

public interface ILocalStreamConsumer<T>
{
    string SubscriptionId { get; }

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
