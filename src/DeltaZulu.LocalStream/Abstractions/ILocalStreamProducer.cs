namespace DeltaZulu.LocalStream;

public interface ILocalStreamProducer<T>
{
    ValueTask<AppendResult> AppendAsync(
        string topic,
        T record,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends a batch with one durable flush per touched segment instead of
    /// one per record. Results are positionally aligned with the input.
    /// Options apply to every record; event IDs are always generated.
    /// </summary>
    ValueTask<IReadOnlyList<AppendResult>> AppendBatchAsync(
        string topic,
        IReadOnlyList<T> records,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask FlushAsync(
        string topic,
        CancellationToken cancellationToken = default);
}
