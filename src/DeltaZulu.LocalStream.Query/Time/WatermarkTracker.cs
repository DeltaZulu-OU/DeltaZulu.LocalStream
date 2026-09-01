namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>
/// Tracks a monotonic event-time watermark across active source partitions.
/// All clock values are supplied by the caller so replay never consults an
/// ambient clock.
/// </summary>
public sealed class WatermarkTracker
{
    private readonly WatermarkPolicy _policy;
    private readonly Dictionary<int, PartitionProgress> _partitions;
    private DateTimeOffset? _watermarkUtc;

    public WatermarkTracker(WatermarkPolicy policy, WatermarkState? restoredState = null)
    {
        ArgumentNullException.ThrowIfNull(policy);
        _policy = policy;
        _watermarkUtc = restoredState?.WatermarkUtc?.ToUniversalTime();
        _partitions = restoredState?.Partitions.ToDictionary(
            state => state.Partition,
            state => new PartitionProgress(
                state.MaxEventTimeUtc.ToUniversalTime(),
                state.LastObservedUtc.ToUniversalTime())) ?? [];
    }

    public DateTimeOffset? WatermarkUtc => _watermarkUtc;

    /// <summary>
    /// Observes a valid event timestamp. Unseen partitions join watermark
    /// calculation on their first observation; old events never move progress
    /// or the published watermark backward.
    /// </summary>
    public DateTimeOffset? Observe(
        int partition,
        DateTimeOffset eventTimeUtc,
        DateTimeOffset observedUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        eventTimeUtc = eventTimeUtc.ToUniversalTime();
        observedUtc = observedUtc.ToUniversalTime();

        if (_partitions.TryGetValue(partition, out var progress))
        {
            _partitions[partition] = progress with
            {
                MaxEventTimeUtc = Max(progress.MaxEventTimeUtc, eventTimeUtc),
                LastObservedUtc = Max(progress.LastObservedUtc, observedUtc),
            };
        }
        else
        {
            _partitions.Add(partition, new PartitionProgress(eventTimeUtc, observedUtc));
        }

        return Advance(observedUtc);
    }

    /// <summary>Re-evaluates idle partitions at a caller-supplied instant.</summary>
    public DateTimeOffset? Advance(DateTimeOffset observedUtc)
    {
        observedUtc = observedUtc.ToUniversalTime();
        var active = _partitions.Values
            .Where(progress => observedUtc - progress.LastObservedUtc < _policy.IdleTimeout)
            .ToArray();

        if (active.Length == 0)
        {
            return _watermarkUtc;
        }

        var minimum = active.Min(progress => progress.MaxEventTimeUtc);
        var candidateTicks = Math.Max(
            DateTimeOffset.MinValue.Ticks,
            minimum.UtcTicks - _policy.OutOfOrderness.Ticks);
        var candidate = new DateTimeOffset(candidateTicks, TimeSpan.Zero);
        if (_watermarkUtc is null || candidate > _watermarkUtc)
        {
            _watermarkUtc = candidate;
        }

        return _watermarkUtc;
    }

    public WatermarkState CaptureState() => new(
        _watermarkUtc,
        _partitions
            .OrderBy(pair => pair.Key)
            .Select(pair => new PartitionWatermarkState(
                pair.Key,
                pair.Value.MaxEventTimeUtc,
                pair.Value.LastObservedUtc))
            .ToArray());

    private static DateTimeOffset Max(DateTimeOffset left, DateTimeOffset right) =>
        left >= right ? left : right;

    private sealed record PartitionProgress(
        DateTimeOffset MaxEventTimeUtc,
        DateTimeOffset LastObservedUtc);
}
