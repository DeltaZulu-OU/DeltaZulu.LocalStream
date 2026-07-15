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

        byte[] payloadBytes;
        try
        {
            payloadBytes = JsonSerializer.SerializeToUtf8Bytes(record);
        }
        catch (NotSupportedException exception)
        {
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.FailedSerialization,
                Reason = exception.Message,
            });
        }

        if (log.Options.MaxRecordBytes is { } maxRecordBytes && payloadBytes.Length > maxRecordBytes)
        {
            return ValueTask.FromResult(new AppendResult
            {
                Status = AppendStatus.RejectedRecordTooLarge,
                Reason = $"Serialized payload is {payloadBytes.Length} bytes; topic '{topic}' allows {maxRecordBytes}.",
            });
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

        JsonElement payload;
        using (var document = JsonDocument.Parse(payloadBytes))
        {
            payload = document.RootElement.Clone();
        }

        var eventId = options?.EventId ?? Guid.NewGuid().ToString("N");
        try
        {
            var offset = log.Append(eventId, DateTimeOffset.UtcNow, options, payload, out var partition);
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

    public ValueTask FlushAsync(string topic, CancellationToken cancellationToken = default)
    {
        // Appends are flushed to disk before AppendAsync returns, so there is
        // no buffered state to flush yet. Kept for API stability.
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        return ValueTask.CompletedTask;
    }
}
