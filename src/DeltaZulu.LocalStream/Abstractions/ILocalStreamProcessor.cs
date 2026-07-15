namespace DeltaZulu.LocalStream;

/// <summary>
/// Context for one <see cref="ILocalStreamProcessor{TIn,TOut}.ProcessAsync"/> call.
/// </summary>
public sealed class ProcessorContext
{
    public required string ProcessorName { get; init; }

    /// <summary>The processor's durable subscription id (<c>processor.&lt;name&gt;</c>).</summary>
    public required string SubscriptionId { get; init; }

    /// <summary>Stream coordinates of the input record being processed.</summary>
    public required StreamPosition InputPosition { get; init; }
}

/// <summary>
/// A local stream processor. The runtime appends output before committing the
/// input offset, so duplicates are possible after a crash: outputs should use
/// deterministic event IDs where possible.
/// </summary>
public interface ILocalStreamProcessor<TIn, TOut>
{
    ValueTask ProcessAsync(
        StreamRecord<TIn> input,
        ILocalStreamProducer<TOut> output,
        ProcessorContext context,
        CancellationToken cancellationToken = default);
}
