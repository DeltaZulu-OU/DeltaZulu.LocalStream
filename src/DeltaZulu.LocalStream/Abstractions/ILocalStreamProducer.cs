namespace DeltaZulu.LocalStream;

public interface ILocalStreamProducer<T>
{
    ValueTask<AppendResult> AppendAsync(
        string topic,
        T record,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask FlushAsync(
        string topic,
        CancellationToken cancellationToken = default);
}
