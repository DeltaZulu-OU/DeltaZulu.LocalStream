using System.Text.Json;
using DeltaZulu.LocalStream.Storage;
using DeltaZulu.LocalStream.Subscriptions;

namespace DeltaZulu.LocalStream;

/// <summary>
/// Owns the local stream runtime: topic metadata, partition segment recovery,
/// subscription checkpoints, retention, and the producer/consumer APIs.
/// </summary>
public sealed class LocalStreamHost : IAsyncDisposable
{
    private static readonly JsonSerializerOptions MetadataJsonOptions = new() { WriteIndented = true };

    private readonly LocalStreamOptions _options;
    private readonly Dictionary<string, TopicLog> _topics = new(StringComparer.Ordinal);

    private SubscriptionStore? _subscriptions;
    private bool _started;

    public LocalStreamHost(LocalStreamOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.StoragePath);
        _options = options;
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
        {
            throw new InvalidOperationException("The host is already started.");
        }

        Directory.CreateDirectory(_options.StoragePath);
        WriteTopicMetadata();

        foreach (var topic in _options.Topics)
        {
            var topicDirectory = Path.Combine(_options.StoragePath, "topics", topic.Name);
            _topics[topic.Name] = new TopicLog(topicDirectory, topic);
        }

        _subscriptions = new SubscriptionStore(_options.StoragePath);
        RegisterConfiguredSubscriptions();
        _started = true;
        return Task.CompletedTask;
    }

    public ILocalStreamProducer<T> CreateProducer<T>()
    {
        EnsureStarted();
        return new LocalStreamProducer<T>(this);
    }

    public ILocalStreamConsumer<T> CreateConsumer<T>(string subscriptionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionId);
        EnsureStarted();
        return new LocalStreamConsumer<T>(this, subscriptionId);
    }

    /// <summary>Runs topic retention policies now, deleting violating sealed segments.</summary>
    public Task ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        var now = DateTimeOffset.UtcNow;
        foreach (var topic in _topics.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            topic.ApplyRetention(now);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Reports whether the subscription's next read position still points at
    /// retained data. <see cref="SubscriptionState.OffsetExpired"/> means
    /// retention deleted past its checkpoint and it must be reset.
    /// </summary>
    public SubscriptionState GetSubscriptionState(string subscriptionId, string topic, int partition)
    {
        EnsureStarted();
        var log = RequireTopic(topic).Partition(partition);
        var committed = _subscriptions!.GetCommittedOffset(subscriptionId, topic, partition);
        if (committed == SubscriptionStore.NoCheckpoint)
        {
            return SubscriptionState.Active;
        }

        return committed + 1 < log.EarliestRetainedOffset
            ? SubscriptionState.OffsetExpired
            : SubscriptionState.Active;
    }

    public ValueTask DisposeAsync()
    {
        _started = false;
        return ValueTask.CompletedTask;
    }

    internal TopicLog? FindTopic(string topic) =>
        _topics.TryGetValue(topic, out var log) ? log : null;

    internal TopicLog RequireTopic(string topic) =>
        FindTopic(topic) ?? throw new ArgumentException($"Unknown topic '{topic}'.", nameof(topic));

    internal SubscriptionStore Subscriptions
    {
        get
        {
            EnsureStarted();
            return _subscriptions!;
        }
    }

    private void EnsureStarted()
    {
        if (!_started)
        {
            throw new InvalidOperationException("The host is not started. Call StartAsync first.");
        }
    }

    private void RegisterConfiguredSubscriptions()
    {
        foreach (var subscription in _options.Subscriptions)
        {
            if (!_topics.TryGetValue(subscription.Topic, out var topic))
            {
                throw new InvalidOperationException(
                    $"Subscription '{subscription.Id}' references unknown topic '{subscription.Topic}'.");
            }

            if (subscription.StartPosition != StartPosition.Latest)
            {
                continue;
            }

            // "Latest" means: skip everything that existed at registration.
            // Only applies before the first checkpoint; configured start
            // positions never reset live subscription progress.
            for (var partition = 0; partition < topic.PartitionCount; partition++)
            {
                var committed = _subscriptions!.GetCommittedOffset(subscription.Id, subscription.Topic, partition);
                var nextOffset = topic.Partition(partition).NextOffset;
                if (committed == SubscriptionStore.NoCheckpoint && nextOffset > 0)
                {
                    _subscriptions.Commit(subscription.Id, subscription.Topic, partition, nextOffset - 1);
                }
            }
        }

        WriteSubscriptionMetadata();
    }

    private void WriteSubscriptionMetadata()
    {
        var metadata = _options.Subscriptions
            .Select(s => new
            {
                id = s.Id,
                topic = s.Topic,
                required = s.Required,
                startPosition = s.StartPosition.ToString(),
            })
            .ToList();

        WriteMetadataFile("subscriptions.json", metadata);
    }

    private void WriteTopicMetadata()
    {
        var metadata = _options.Topics
            .Select(t => new
            {
                name = t.Name,
                partitions = t.Partitions,
                maxSegmentBytes = t.MaxSegmentBytes,
                retention = new { maxBytes = t.Retention.MaxBytes, maxAge = t.Retention.MaxAge?.ToString() },
            })
            .ToList();

        WriteMetadataFile("topics.json", metadata);
    }

    private void WriteMetadataFile(string fileName, object metadata)
    {
        var metadataDirectory = Path.Combine(_options.StoragePath, "metadata");
        Directory.CreateDirectory(metadataDirectory);

        var path = Path.Combine(metadataDirectory, fileName);
        var temp = path + ".tmp";
        File.WriteAllText(temp, JsonSerializer.Serialize(metadata, MetadataJsonOptions));
        File.Move(temp, path, overwrite: true);
    }
}
