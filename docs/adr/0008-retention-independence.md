# ADR-0008: Retention Policy — Independent of Subscription Progress

**Status:** Accepted

**Date:** 2026-07-17

## Context

Different stream systems handle retention differently:

| System | Retention Trigger | Consequence |
|---|---|---|
| **Kafka** | Retention policy (time/size) OR consumer group lag | Subscriptions can fall behind and expire |
| **RabbitMQ** | Consumer completion | No retention if consumer is lagging |
| **Pulsar** | Retention policy (time/size) independent of subscriptions | Subscriptions can lag indefinitely |
| **DurableBuffer** | Consumer completion | Records deleted when consumed |

LocalStream must choose a model that balances:

1. **Data durability** — Archive subscriber must never lose records
2. **Operational flexibility** — Optional subscribers (logcluster) can lag or drop
3. **Cost containment** — Disk space must not grow unbounded
4. **Simplicity** — Retention should not require consensus or subscription state

The core conflict: If retention waits for the slowest subscription to progress, retention is complex (must track all subscriptions). If retention is purely time/size based, fast subscriptions hold old data.

### Solution Rationale

Retention should be **independent of subscription progress** because:

- **Archive subscription is critical** — Must not block due to lagging optional subscribers
- **Retention is a policy** — Like "keep 7 days or 20 GB", not a consumer-driven action
- **Lag is metadata, not data** — If a subscription falls behind retention, that's a late alert; doesn't mean data wasn't published
- **Simplicity** — No need to track subscription state during retention; just check time and size

## Decision

**Retention is triggered by policy (MaxBytes or MaxAge), independent of any subscription's progress.**

### Retention Policy Example

```yaml
localStream:
  topics:
    - name: agent.output
      partitions: 4
      retention:
        maxBytes: 21474836480  # 20 GB
        maxAge: 7d              # 7 days
        deletePolicy: delete    # or: compact (future)
```

### Retention Enforcement

```csharp
public void ApplyRetention(RetentionOptions retention, DateTimeOffset nowUtc)
{
    lock (_sync)
    {
        while (_segments.Count > 1 && ViolatesPolicy(_segments, retention, nowUtc))
        {
            var oldest = _segments[0];
            File.Delete(oldest.Path);
            _segments.RemoveAt(0);
        }
    }
}

private static bool ViolatesPolicy(
    List<Segment> segments,
    RetentionOptions retention,
    DateTimeOffset nowUtc)
{
    // MaxBytes violation
    if (retention.MaxBytes is { } maxBytes)
    {
        var totalBytes = segments.Sum(s => s.SizeBytes);
        if (totalBytes > maxBytes)
            return true;
    }

    // MaxAge violation
    if (retention.MaxAge is { } maxAge)
    {
        var oldest = segments[0];
        var age = nowUtc - oldest.FirstRecordUtc;
        if (age > maxAge)
            return true;
    }

    return false;
}
```

### OffsetExpired State

If a subscription's committed offset points to deleted data:

```csharp
public sealed record OffsetExpiredEvent
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long CommittedOffset { get; init; }
    public required long EarliestRetainedOffset { get; init; }
}
```

The subscription enters `OffsetExpired` state. The operator must reset it:

```csharp
public async ValueTask ResetAsync(
    string topic,
    ResetPosition position)  // earliest, latest, specific offset, specific timestamp
{
    // ...
}
```

### Backpressure Levels

Retention independence allows backpressure at each edge:

| Component | Behavior | Motivation |
|---|---|---|
| **Producer (Agent)** | Block/reject on MaxTotalBytes or MaxRecordBytes | Protect broker |
| **Archive subscriber** | Required; alert and retry | Must never lose data |
| **Silver subscriber** | Usually required; configurable | NRT completeness expected |
| **LogCluster subscriber** | Optional; sample/drop under pressure | Quality-of-service trade-off |

Archive can progress while logcluster lags. Retention will not hold data for logcluster.

## Consequences

### Positive

- ✓ Archive durability independent of subscriber lag
- ✓ Optional subscribers can drop or sample without affecting data retention
- ✓ Retention policy is simple (no subscription state needed)
- ✓ Scaling is predictable (disk usage bounded by retention policy, not subscriptions)
- ✓ Avoids cascading failures (slow subscriber doesn't hold data for everyone)
- ✓ Matches Kafka/Pulsar model (retention-based, not completion-based)

### Negative / Trade-offs

- ⚠ **Subscription can expire:** Lagging subscribers may fall behind retention
  - *Mitigation:* Acceptable; operator can reset subscription. Archive durability is protected by requiring archive subscription.
  
- ⚠ **Requires explicit reset:** Operator must decide where to resume (earliest, latest, or specific offset)
  - *Mitigation:* Provide CLI tools and operational dashboards. Alert when OffsetExpired occurs.
  
- ⚠ **Lag visibility is critical:** If a subscription lags silently, operator may not notice until it expires
  - *Mitigation:* Expose metrics (subscription lag, age of oldest uncommitted record). Set up alerts.

## Alternatives Considered

1. **Retention waits for slowest subscription:**
   - Pro: No subscription expiration
   - Con: Archive durability depends on optional subscribers; lagging debug subscriber blocks retention
   - Decision: Rejected; creates coupling

2. **Different retention policies per subscription:**
   - Pro: Flexibility (archive keeps 30 days, logcluster keeps 3 days)
   - Con: Complex to implement; confusing API; difficult to reason about
   - Decision: Rejected; premature complexity

3. **TTL + subscription progress (hybrid):**
   - Pro: Prevents unbounded lag
   - Con: Still blocks archive if subscriber is slow; adds complexity
   - Decision: Rejected; doesn't solve the problem

4. **No retention, rely on manual cleanup:**
   - Pro: Simple
   - Con: Disk will fill; operator must remember; system crashes
   - Decision: Rejected; unacceptable for production

## Design Notes

### Why Not "Subscriber-Aware Retention"?

A common pattern in distributed systems is to retain data until the slowest subscriber consumes it. This is appealing because:
- No subscription expiration
- Automatic cleanup
- Matches some RabbitMQ queue models

But it fails for LocalStream because:
- Archive is critical; optional subscribers (logcluster) must not block it
- Lagging debug subscriber on a development machine can hold production data indefinitely
- Retention becomes non-deterministic (depends on external subscription state)

With policy-based retention:
- Archive subscribers should monitor lag and alert
- Retention is explicit and predictable
- Operator has control (can manually reset lagging subscriptions)

### OffsetExpired as an Explicit State

Rather than silently jumping a subscription to the nearest available offset, LocalStream makes expiration explicit:

```csharp
public enum SubscriptionState
{
    Active,
    OffsetExpired,      // ← Explicit; operator must decide
    Paused,
}
```

This forces:
- Operator awareness (not silent skips)
- Intentional recovery (not default behavior)
- Auditability (log the reset action and reason)

### Retention Audit

Provide an audit tool to check:
- Topic size and record count
- Segment ages
- Subscription lags
- Which subscriptions will expire at next retention run

```csharp
public sealed record RetentionAudit
{
    public required long TotalBytes { get; init; }
    public required int SegmentCount { get; init; }
    public required DateTimeOffset OldestRecordUtc { get; init; }
    public required DateTimeOffset NewestRecordUtc { get; init; }
    
    public required Dictionary<string, SubscriptionLag> SubscriptionLags { get; init; }
}

public sealed record SubscriptionLag
{
    public required long CommittedOffset { get; init; }
    public required long LatestOffset { get; init; }
    public required long BytesLag { get; init; }
    public required bool WillExpireNextRun { get; init; }
}
```

## Related Decisions

- **ADR-0005:** DurableBuffer boundary (DurableBuffer uses completion-based deletion; LocalStream uses retention policy)
- **ADR-0002:** Retention complexity optimization (O(n²) → O(n) implementation detail)
- **ADR-0006:** Delivery semantics (at-least-once, not exactly-once, tolerates subscription lag)

## Testing

- ✓ Retention deletes oldest segment when MaxBytes exceeded
- ✓ Retention deletes oldest segment when MaxAge exceeded
- ✓ Lagging subscription can still read if offset is not yet deleted
- ✓ Lagging subscription enters OffsetExpired when offset is deleted
- ✓ Archive subscription progress does not prevent logcluster retention
- ✓ Reset to earliest recovers from OffsetExpired
- ✓ Reset to latest skips to newest record
- ✓ Manual reset to specific offset is durable

## Notes for Future Maintainers

**If considering subscription-aware retention:**
- Measure first: How often do subscriptions actually lag beyond retention?
- Weigh the cost: How much complexity is it worth to avoid resetting subscriptions?
- Consider: Could alerts and dashboard visibility be better than automatic retention hold?

**If adding compaction:**
- Retention policy can include `deletePolicy: compact` (future)
- Compaction keeps only latest value per key; different retention model
- Should still be independent of subscription progress

**For operational runbooks:**
- Document how to detect OffsetExpired (metrics or log scan)
- Document how to reset a subscription
- Document expected lag thresholds for each subscriber type
