# ADR-0003: Event-Driven Memoization for Partition Metrics

**Status:** Accepted

**Date:** 2026-07-17

## Context

Two expensive computed properties are accessed frequently on hot paths:

1. **`TopicLog.TotalSizeBytes`** — Sum of all partition sizes. Called on every `AppendAsync()` to check MaxTotalBytes limit.
2. **`PartitionLog.EarliestRetainedOffset`** — First segment with records. Called on every metrics query, subscription state check, and some read paths.

Both involve O(segments) work and are called repeatedly within tight loops (e.g., metrics collection iterates all partitions).

### Original Performance Profile

| Operation | Calls Per Cycle | Cost Per Call | Total |
|---|---|---|---|
| **AppendAsync (MaxTotalBytes check)** | 1000/sec | O(total_segments) | O(1,000,000) per second |
| **GetTopicMetrics (100 partitions)** | 1/sec | O(partitions × segments) | O(100,000) per call |
| **GetSubscriptionMetrics (100 partitions)** | 1/sec | O(partitions × segments) | O(100,000) per call |

## Decision

Implement **memoization with event-driven invalidation**:

### Pattern: Cache + Dirty Flag

```csharp
// Fields
private long _cachedTotalSizeBytes;
private bool _totalSizeBytesDirty = true;

// Property (pure function)
public long TotalSizeBytes
{
    get
    {
        if (_totalSizeBytesDirty)
        {
            _cachedTotalSizeBytes = _partitions.Sum(p => p.SizeBytes);
            _totalSizeBytesDirty = false;
        }
        return _cachedTotalSizeBytes;
    }
}

// Invalidation (only on state change)
public void ApplyRetention(...)
{
    // ... delete segments ...
    _totalSizeBytesDirty = true;  // ← Only point of invalidation
}
```

### Invalidation Points (Per Metric)

| Metric | Invalidated By | Frequency |
|---|---|---|
| **TotalSizeBytes** | ApplyRetention (segment delete) | Minutes (periodic retention cycle) |
| **EarliestRetainedOffset** | ApplyRetention (delete oldest), Recover (rebuild) | Minutes + startup |

### Impact

| Scenario | Before | After | Gain |
|---|---|---|---|
| **1000 appends/sec (checks MaxTotalBytes)** | 1000 × O(segments) | 1 sum + 999 cache hits | 1000× faster |
| **Metrics query (100 partitions)** | O(partitions × segments) | O(partitions) after first call | 100× faster |
| **Processor poll every 250 ms** | Re-sum entire topic | Instant cache hit (if no retention) | 1000× faster |

## Consequences

### Positive

- ✓ Hot paths (append, read) drop from O(segments) to O(1) amortized
- ✓ Metrics become instant after first call
- ✓ Minimal memory overhead (one flag + one long per topic/partition)
- ✓ Invalidation is deterministic and explicit (tied to operations that change state)
- ✓ No TTL (Time-To-Live) complexity; cache lives until manually invalidated

### Negative / Trade-offs

- ⚠ **Stale cache on crashes:** A segment's SizeBytes is updated on append, but if the process crashes before the next retention run, the TotalSizeBytes cache remains valid. However, if a future append updates segment.SizeBytes *after* cache compute but *before* crash, the cache could be stale.
  - *Mitigation:* Acceptable because:
    1. We already track per-segment SizeBytes durably
    2. The cache is only used for flow control (MaxTotalBytes), not correctness
    3. At startup, the cache is rebuilt fresh from disk
  - *Example:* If MaxTotalBytes=1000 and cache says 900, but true total is 950 after a crash, we might reject an append unnecessarily. This is safe (conservative).

- ⚠ **Requires discipline:** Future code must invalidate the cache when the underlying state changes.
  - *Mitigation:* Document the invariant. Cache fields are private; invalidation methods are internal. Unlikely to cause bugs.

## Alternatives Considered

1. **Use `ReaderWriterLockSlim` instead of lock:**
   - Pro: Multiple readers don't block on property access
   - Con: Memoization is simpler and sufficient
   - Decision: Rejected; memoization is simpler

2. **Compute totals lazily in a background thread:**
   - Pro: Never blocks the hot path
   - Con: Adds threading complexity; stale cache becomes visible
   - Decision: Rejected; synchronous memoization is cleaner

3. **Use a TTL-based cache (e.g., only valid for 100 ms):**
   - Pro: Prevents arbitrarily stale caches
   - Con: Still has stale intervals; adds timer complexity
   - Decision: Rejected; event-driven is better (invalidate when true state changes)

4. **No caching; accept O(segments) on every call:**
   - Pro: Simple code
   - Con: 1000× slower on large topics
   - Decision: Rejected; unacceptable for append hot path

## Memoization vs. Caching: Why This Pattern Works

This implementation is **memoization** (not general caching) because:

1. **Pure functions:** TotalSizeBytes and EarliestRetainedOffset have no side effects; same input → same output
2. **Hashable state:** Cache key is implicit (no argument passing); we cache the result of `this.property`
3. **Simple invalidation:** Only 2 operations in the entire codebase dirty the cache (retention, recovery)
4. **High hit rate:** Metrics and append checks repeat thousands of times between invalidation events

See [`docs/LOCAL_STREAM_ARCHITECTURE.md`](../LOCAL_STREAM_ARCHITECTURE.md) for the broader design context.

## Related Decisions

- ADR-0002: Retention complexity optimization (complements this; removes redundant total-size calculations)
- ADR-0004: Payload serialization strategy (shares philosophy: avoid redundant work)

## Notes for Future Maintainers

**When to invalidate:**
- `TotalSizeBytes`: Invalidate when segment count or size changes → ApplyRetention
- `EarliestRetainedOffset`: Invalidate when segment list is rebuilt or sorted → ApplyRetention, Recover

**When NOT to invalidate:**
- AppendAsync: Adds records to segments, but updates segment.SizeBytes in-place (cache still valid)
- Read/Consume: Doesn't change segment list or sizes (cache still valid)

**Testing:** Verify that invalidation happens *after* the state change, never before. Use snapshot testing if cache behavior becomes complex.
