using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;

namespace DeltaZulu.LocalStream.Subscriptions;

/// <summary>
/// Durable per-subscription committed offsets. One checkpoint file per
/// subscription, topic, and partition; writes are atomic (temp + rename).
/// A committed offset of -1 means "no checkpoint".
/// </summary>
internal sealed class SubscriptionStore(string rootDirectory)
{
    public const long NoCheckpoint = -1;

    private sealed record CheckpointDocument(long CommittedOffset, DateTimeOffset UpdatedUtc);

    private readonly ConcurrentDictionary<(string Subscription, string Topic, int Partition), long> _cache = new();

    public long GetCommittedOffset(string subscriptionId, string topic, int partition)
    {
        return _cache.GetOrAdd((subscriptionId, topic, partition), key =>
        {
            var path = CheckpointPath(key.Subscription, key.Topic, key.Partition);
            if (!File.Exists(path))
            {
                return NoCheckpoint;
            }

            var document = JsonSerializer.Deserialize<CheckpointDocument>(File.ReadAllText(path));
            return document?.CommittedOffset ?? NoCheckpoint;
        });
    }

    public void Commit(string subscriptionId, string topic, int partition, long offset)
    {
        var path = CheckpointPath(subscriptionId, topic, partition);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        var document = new CheckpointDocument(offset, DateTimeOffset.UtcNow);
        var temp = path + ".tmp";
        File.WriteAllText(temp, JsonSerializer.Serialize(document));
        File.Move(temp, path, overwrite: true);

        _cache[(subscriptionId, topic, partition)] = offset;
    }

    public void ClearCheckpoint(string subscriptionId, string topic, int partition)
    {
        var path = CheckpointPath(subscriptionId, topic, partition);
        File.Delete(path);
        _cache[(subscriptionId, topic, partition)] = NoCheckpoint;
    }

    private string CheckpointPath(string subscriptionId, string topic, int partition) =>
        Path.Combine(
            rootDirectory,
            "subscriptions",
            subscriptionId,
            topic,
            partition.ToString("D6", CultureInfo.InvariantCulture) + ".checkpoint");
}
