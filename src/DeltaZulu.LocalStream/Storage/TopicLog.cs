using System.Globalization;
using System.Text.Json;

namespace DeltaZulu.LocalStream.Storage;

/// <summary>A topic: a fixed set of partition logs plus partition routing.</summary>
internal sealed class TopicLog
{
    private readonly PartitionLog[] _partitions;
    private int _roundRobin = -1;

    public TopicLog(string topicDirectory, TopicOptions options)
    {
        Options = options;
        _partitions = new PartitionLog[options.Partitions];
        for (var i = 0; i < options.Partitions; i++)
        {
            var partitionDirectory = Path.Combine(
                topicDirectory,
                "partitions",
                i.ToString("D6", CultureInfo.InvariantCulture));
            _partitions[i] = new PartitionLog(partitionDirectory, options.MaxSegmentBytes);
        }
    }

    public TopicOptions Options { get; }

    public int PartitionCount => _partitions.Length;

    public PartitionLog Partition(int index) => _partitions[index];

    public long Append(
        string eventId,
        DateTimeOffset publishedUtc,
        AppendOptions? options,
        JsonElement payload,
        out int partition)
    {
        partition = SelectPartition(options);
        return _partitions[partition].Append(eventId, publishedUtc, options?.Headers, payload);
    }

    public void ApplyRetention(DateTimeOffset nowUtc)
    {
        foreach (var partition in _partitions)
        {
            partition.ApplyRetention(Options.Retention, nowUtc);
        }
    }

    private int SelectPartition(AppendOptions? options)
    {
        if (options?.Partition is { } explicitPartition)
        {
            if (explicitPartition < 0 || explicitPartition >= _partitions.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    explicitPartition,
                    $"Topic '{Options.Name}' has {_partitions.Length} partitions.");
            }

            return explicitPartition;
        }

        if (options?.PartitionKey is { } key)
        {
            return StableHash(key) % _partitions.Length;
        }

        return (int)((uint)Interlocked.Increment(ref _roundRobin) % _partitions.Length);
    }

    /// <summary>FNV-1a; must stay stable across processes and runtime versions.</summary>
    private static int StableHash(string key)
    {
        var hash = 2166136261u;
        foreach (var c in key)
        {
            hash ^= c;
            hash *= 16777619u;
        }

        return (int)(hash & 0x7FFFFFFF);
    }
}
