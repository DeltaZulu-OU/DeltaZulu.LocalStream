# ADR-0002: Retention Policy Complexity — O(n²) to O(n)

**Status:** Accepted

**Date:** 2026-07-17

## Context

`ApplyRetention()` and `AuditRetention()` delete oldest segments that violate retention policy (MaxBytes or MaxAge). The methods loop through segments and call `ViolatesPolicy()` on each iteration.

### Original Implementation

```csharp
while (_segments.Count > 1 && ViolatesPolicy(_segments, retention, nowUtc))
{
    File.Delete(_segments[0].Path);
    _segments.RemoveAt(0);
}

private static bool ViolatesPolicy(List<Segment> segments, ...)
{
    if (retention.MaxBytes is { } maxBytes && segments.Sum(s => s.SizeBytes) > maxBytes)
        return true;
    // ...
}
```

### Complexity Analysis

**For a topic with 1000 segments, deleting 500 to meet policy:**

- Iteration 1: `Sum(1000 segments)`
- Iteration 2: `Sum(999 segments)`
- ...
- Iteration 500: `Sum(501 segments)`

**Total: 1000 + 999 + ... + 501 = ~750,000 segment iterations** just to check the size cap.

**Complexity class: O(n²) where n = segments deleted.**

This is acceptable for small segment counts (< 100) but becomes noticeable for large topics:
- 10,000 segments, delete 5,000 = ~37.5M iterations
- Run time: ~100 ms on modern CPU (not catastrophic, but wasteful)

## Decision

**Pre-compute total bytes once, decrement as segments are deleted:**

```csharp
public void ApplyRetention(RetentionOptions retention, DateTimeOffset nowUtc)
{
    lock (_sync)
    {
        var totalBytes = _segments.Sum(s => s.SizeBytes);  // ← O(n) once
        while (_segments.Count > 1 && ViolatesPolicy(_segments, retention, nowUtc, totalBytes))
        {
            var oldest = _segments[0];
            totalBytes -= oldest.SizeBytes;  // ← O(1) decrement
            File.Delete(oldest.Path);
            _segments.RemoveAt(0);
        }
    }
}

private static bool ViolatesPolicy(
    List<Segment> segments,
    RetentionOptions retention,
    DateTimeOffset nowUtc,
    long totalBytes)  // ← Pass as parameter
{
    if (retention.MaxBytes is { } maxBytes && totalBytes > maxBytes)
        return true;
    // ...
}
```

### Impact

| Scenario | Before | After | Gain |
|---|---|---|---|
| **1000 segments, delete 500** | ~750K sums | 1 sum + 500 decrements | 1500× faster |
| **10K segments, delete 5K** | ~37.5M sums | 1 sum + 5K decrements | 7500× faster |
| **Small topics (100 segments)** | 5,000 sums | 1 sum + 50 decrements | Negligible improvement |

## Consequences

### Positive

- ✓ Retention policy checks drop from O(n²) to O(n)
- ✓ Large topics (1000+ segments) see dramatic speedup
- ✓ No memory overhead (decrement is O(1) space)
- ✓ Logic remains simple and understandable

### Negative / Trade-offs

- ⚠ **Slight API change:** `ViolatesPolicy()` now takes `totalBytes` parameter. Internal detail, no external impact.
- ⚠ **Manual management:** If future code adds or removes segments outside `ApplyRetention`, the total must be manually updated
  - *Mitigation:* Segment changes only happen in `ApplyRetention`, `Recover`, and `ActiveSegmentForWrite` (append). Document this invariant.

## Alternatives Considered

1. **Cache segment total size and invalidate on changes:**
   - Pro: No parameter passing; ViolatesPolicy stays simple
   - Con: Cache invalidation adds complexity; same asymptotic improvement
   - Decision: Rejected; passing totalBytes is simpler and more explicit

2. **Use a SortedList or PriorityQueue for segments:**
   - Pro: O(1) min/max deletion
   - Con: Adds complexity for a rare operation; retention is not hot path
   - Decision: Rejected; premature optimization

3. **Keep O(n²) but run retention less frequently:**
   - Pro: Defer the problem
   - Con: Doesn't fix the algorithmic flaw; still problematic at scale
   - Decision: Rejected; fix the root cause

## Related Decisions

- See ADR-0003 for TotalSizeBytes caching (complements this fix on the append side)

## Notes

Retention is not a hot path, so this fix is a medium-priority optimization. However, it eliminates a clear algorithmic inefficiency and should be applied as part of general code hygiene.
