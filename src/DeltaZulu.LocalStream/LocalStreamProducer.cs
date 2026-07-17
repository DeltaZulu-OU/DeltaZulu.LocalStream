using System.Text.Json;

namespace DeltaZulu.LocalStream;

internal sealed class LocalStreamProducer<T>(LocalStreamHost host) : ILocalStreamProducer<T>
{
    public ValueTask<AppendResult> AppendAsync(
        string topic,
        T record,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromResult(new AppendResult { Status = AppendStatus.Cancelled });
        }

        var log = host.FindTopic(topic);
        if (log is null)
        {
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.RejectedTopicNotFound,
                Reason = $"Topic '{topic}' is not configured.",
            });
        }

        if (!TrySerializePayload(log, topic, record, out var payloadBytes, out var failure))
        {
            return ValueTask.FromResult(failure);
        }

        if (log.Options.MaxTotalBytes is { } maxTotalBytes && log.TotalSizeBytes >= maxTotalBytes)
        {
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.RejectedStreamFull,
                Reason = $"Topic '{topic}' holds {log.TotalSizeBytes} bytes, at or above its {maxTotalBytes}-byte cap. " +
                    "Retention must free sealed segments before appends resume.",
            });
        }

        var eventId = options?.EventId ?? Guid.NewGuid().ToString("N");
        try
        {
            var offset = log.Append(eventId, DateTimeOffset.UtcNow, options, payloadBytes, out var partition);
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.Appended,
                Position = new StreamPosition(topic, partition, offset),
                EventId = eventId,
            });
        }
        catch (IOException exception)
        {
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.FailedStorage,
                EventId = eventId,
                Reason = exception.Message,
            });
        }
    }

    public ValueTask<IReadOnlyList<AppendResult>> AppendBatchAsync(
        string topic,
        IReadOnlyList<T> records,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(records);

        var results = new AppendResult[records.Count];

        if (cancellationToken.IsCancellationRequested)
        {
            return Fill(results, new AppendResult { Status = AppendStatus.Cancelled });
        }

        var log = host.FindTopic(topic);
        if (log is null)
        {
            return Fill(results, new AppendResult
            {
                Status = AppendStatus.RejectedTopicNotFound,
                Reason = $"Topic '{topic}' is not configured.",
            });
        }

        if (log.Options.MaxTotalBytes is { } maxTotalBytes && log.TotalSizeBytes >= maxTotalBytes)
        {
            return Fill(results, new AppendResult
            {
                Status = AppendStatus.RejectedStreamFull,
                Reason = $"Topic '{topic}' holds {log.TotalSizeBytes} bytes, at or above its {maxTotalBytes}-byte cap.",
            });
        }

        var publishedUtc = DateTimeOffset.UtcNow;
        var pendingIndexes = new List<int>(records.Count);
        var pending = new List<Storage.PartitionLog.PendingRecord>(records.Count);
        for (var i = 0; i < records.Count; i++)
        {
            if (!TrySerializePayload(log, topic, records[i], out var payloadBytes, out var failure))
            {
                results[i] = failure;
                continue;
            }

            pendingIndexes.Add(i);
            pending.Add(new Storage.PartitionLog.PendingRecord(
                Guid.NewGuid().ToString("N"),
                publishedUtc,
                options?.Headers,
                payloadBytes));
        }

        try
        {
            var positions = log.AppendBatch(pending, options);
            for (var i = 0; i < pendingIndexes.Count; i++)
            {
                results[pendingIndexes[i]] = new AppendResult
                {
                    Status = AppendStatus.Appended,
                    Position = new StreamPosition(topic, positions[i].Partition, positions[i].Offset),
                    EventId = pending[i].EventId,
                };
            }
        }
        catch (IOException exception)
        {
            for (var i = 0; i < pendingIndexes.Count; i++)
            {
                results[pendingIndexes[i]] ??= new AppendResult
                {
                    Status = AppendStatus.FailedStorage,
                    Reason = exception.Message,
                };
            }
        }

        return ValueTask.FromResult<IReadOnlyList<AppendResult>>(results);
    }

    /// <summary>
    /// Serializes a record's payload and enforces the per-record size cap.
    /// Returns false with a populated <paramref name="failure"/> on a
    /// serialization error or an oversized payload.
    /// </summary>
    private static bool TrySerializePayload(
        Storage.TopicLog log,
        string topic,
        T record,
        out byte[] payload,
        out AppendResult failure)
    {
        try
        {
            payload = JsonSerializer.SerializeToUtf8Bytes(record);
        }
        catch (NotSupportedException exception)
        {
            payload = [];
            failure = new AppendResult
            {
                Status = AppendStatus.FailedSerialization,
                Reason = exception.Message,
            };
            return false;
        }

        if (log.Options.MaxRecordBytes is { } maxRecordBytes && payload.Length > maxRecordBytes)
        {
            failure = new AppendResult
            {
                Status = AppendStatus.RejectedRecordTooLarge,
                Reason = $"Serialized payload is {payload.Length} bytes; topic '{topic}' allows {maxRecordBytes}.",
            };
            return false;
        }

        failure = null!;
        return true;
    }

    private static ValueTask<IReadOnlyList<AppendResult>> Fill(AppendResult[] results, AppendResult value)
    {
        Array.Fill(results, value);
        return ValueTask.FromResult<IReadOnlyList<AppendResult>>(results);
    }

    public ValueTask FlushAsync(string topic, CancellationToken cancellationToken = default)
    {
        // Appends are flushed to disk before AppendAsync returns, so there is
        // no buffered state to flush yet. Kept for API stability.
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        return ValueTask.CompletedTask;
    }
}
