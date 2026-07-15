using System.Runtime.CompilerServices;
using System.Text.Json;
using DeltaZulu.LocalStream.Storage;
using DeltaZulu.LocalStream.Subscriptions;

namespace DeltaZulu.LocalStream;

internal sealed class LocalStreamConsumer<T>(LocalStreamHost host, string subscriptionId)
    : ILocalStreamConsumer<T>
{
    public string SubscriptionId { get; } = subscriptionId;

    public async IAsyncEnumerable<StreamRecord<T>> ReadAsync(
        string topic,
        ReadOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var log = host.RequireTopic(topic);
        options ??= new ReadOptions();

        var partitions = options.Partition is { } single
            ? [single]
            : Enumerable.Range(0, log.PartitionCount);

        foreach (var partition in partitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var partitionLog = log.Partition(partition);
            var fromOffset = ResolveStartOffset(topic, partition, partitionLog, options.Start);

            foreach (var envelope in partitionLog.Read(fromOffset))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return ToRecord(topic, partition, envelope);
            }
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public ValueTask CommitAsync(StreamPosition position, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(position);
        _ = host.RequireTopic(position.Topic);
        host.Subscriptions.Commit(SubscriptionId, position.Topic, position.Partition, position.Offset);
        return ValueTask.CompletedTask;
    }

    public ValueTask ResetAsync(
        string topic,
        ResetPosition position,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(position);
        var log = host.RequireTopic(topic);

        for (var partition = 0; partition < log.PartitionCount; partition++)
        {
            var partitionLog = log.Partition(partition);
            switch (position.Kind)
            {
                case ResetKind.Earliest:
                    // No checkpoint means "start from earliest retained at read
                    // time", which also clears OffsetExpired.
                    host.Subscriptions.ClearCheckpoint(SubscriptionId, topic, partition);
                    break;

                case ResetKind.Latest:
                    host.Subscriptions.Commit(SubscriptionId, topic, partition, partitionLog.NextOffset - 1);
                    break;

                case ResetKind.Offset:
                    CommitBefore(topic, partition, position.Offset);
                    break;

                case ResetKind.Timestamp:
                    var offset = partitionLog.FindOffsetByTimestamp(position.Timestamp)
                        ?? partitionLog.NextOffset;
                    CommitBefore(topic, partition, offset);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(position));
            }
        }

        return ValueTask.CompletedTask;
    }

    private void CommitBefore(string topic, int partition, long nextOffsetToRead)
    {
        if (nextOffsetToRead <= 0)
        {
            host.Subscriptions.ClearCheckpoint(SubscriptionId, topic, partition);
        }
        else
        {
            host.Subscriptions.Commit(SubscriptionId, topic, partition, nextOffsetToRead - 1);
        }
    }

    private long ResolveStartOffset(string topic, int partition, PartitionLog partitionLog, ReadStart start)
    {
        switch (start)
        {
            case ReadStart.Earliest:
                return partitionLog.EarliestRetainedOffset;

            case ReadStart.Latest:
                return partitionLog.NextOffset;

            case ReadStart.Committed:
                var committed = host.Subscriptions.GetCommittedOffset(SubscriptionId, topic, partition);
                if (committed == SubscriptionStore.NoCheckpoint)
                {
                    return partitionLog.EarliestRetainedOffset;
                }

                var next = committed + 1;
                var earliest = partitionLog.EarliestRetainedOffset;
                if (next < earliest)
                {
                    throw new OffsetExpiredException(SubscriptionId, topic, partition, committed, earliest);
                }

                return next;

            default:
                throw new ArgumentOutOfRangeException(nameof(start));
        }
    }

    private static StreamRecord<T> ToRecord(string topic, int partition, RecordEnvelope envelope)
    {
        var payload = envelope.Payload.Deserialize<T>()
            ?? throw new JsonException($"Record at {topic}/{partition}/{envelope.Offset} deserialized to null.");

        return new StreamRecord<T>
        {
            Topic = topic,
            Partition = partition,
            Offset = envelope.Offset,
            EventId = envelope.EventId,
            PublishedUtc = envelope.PublishedUtc,
            Headers = envelope.Headers,
            Payload = payload,
        };
    }
}
