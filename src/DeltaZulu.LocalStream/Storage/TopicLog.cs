using System.Globalization;

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

    public long TotalSizeBytes => _partitions.Sum(p => p.SizeBytes);

    public PartitionLog Partition(int index) => _partitions[index];

    public long Append(
        string eventId,
        DateTimeOffset publishedUtc,
        AppendOptions? options,
        byte[] payloadJson,
        out int partition)
    {
        partition = SelectPartition(options);
        return _partitions[partition].Append(eventId, publishedUtc, options?.Headers, payloadJson);
    }

    /// <summary>
    /// Routes and appends a batch. Records are grouped per partition in input
    /// order and each partition group is written with one durable flush per
    /// touched segment. Returns (partition, offset) aligned with the input.
    /// </summary>
    public IReadOnlyList<(int Partition, long Offset)> AppendBatch(
        IReadOnlyList<PartitionLog.PendingRecord> records,
        AppendOptions? options)
    {
        var routed = new int[records.Count];
        var groups = new Dictionary<int, List<int>>();
        for (var i = 0; i < records.Count; i++)
        {
            var partition = SelectPartition(options);
            routed[i] = partition;
            if (!groups.TryGetValue(partition, out var indexes))
            {
                indexes = [];
                groups[partition] = indexes;
            }

            indexes.Add(i);
        }

        var positions = new (int Partition, long Offset)[records.Count];
        foreach (var (partition, indexes) in groups)
        {
            var group = indexes.Select(i => records[i]).ToList();
            var firstOffset = _partitions[partition].AppendMany(group);
            for (var i = 0; i < indexes.Count; i++)
            {
                positions[indexes[i]] = (partition, firstOffset + i);
            }
        }

        return positions;
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
