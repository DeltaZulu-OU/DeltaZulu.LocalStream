# ADR-0007: Record Identity — EventId and Stream Coordinates Design

**Status:** Accepted

**Date:** 2026-07-17

## Context

Stream processors need to identify and deduplicate records. Two identity models are common:

1. **Cross-system ID (semantic identity):** EventId or CorrelationId assigned by producer
   - Unique within the logical event domain (e.g., "agent-run-12345")
   - Stable across republishes, retries, and system boundaries
   - Understood by sinks (Data Lake, NRT processors)

2. **Stream coordinates (positional identity):** Topic + Partition + Offset
   - Unique within LocalStream
   - Immutable, assigned at append time
   - Forms the replay key for subscriptions

Stream records needed to carry both for different use cases:

- **Cross-system deduplication:** EventId (when exactly-once semantics needed at sink)
- **Local replay and recovery:** Stream coordinates (when restarting subscription)
- **Debuggability:** Both (to correlate local records with external logs)

Without both, either:
- Sink cannot deduplicate on external output (duplicate records in Data Lake)
- Subscription cannot resume correctly after offset expiration
- Logs are harder to debug (no connection between local and external systems)

## Decision

**Define `StreamRecord<T>` to carry both EventId and stream coordinates:**

```csharp
public sealed record StreamRecord<T>
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string EventId { get; init; }
    public required DateTimeOffset PublishedUtc { get; init; }
    public required IReadOnlyDictionary<string, string>? Headers { get; init; }
    public required T Payload { get; init; }
}
```

### Producer Responsibility

Producers assign `EventId` at append time:

```csharp
var result = await producer.AppendAsync(
    topic: "agent.output",
    record: new AgentEvent { Source = "network", ... },
    options: new AppendOptions { EventId = "agent-run-12345" }
);

if (result.Status == AppendStatus.Appended)
{
    // EventId was persisted
    // result.Position = new StreamPosition(Topic, Partition, Offset)
}
```

EventId should be:
- **Stable:** Assigned by upstream system (not random)
- **Debuggable:** Meaningful (e.g., "scan-job-1001", not "abc123xyz")
- **Deduplication-ready:** Unique across retries and republishes

### Subscription Responsibility

Subscriptions read records with both identities:

```csharp
var archive = host.CreateConsumer<AgentEvent>("archive");
await foreach (var record in archive.ReadAsync("agent.output"))
{
    Console.WriteLine($"{record.EventId} @ {record.Topic}/{record.Partition}/{record.Offset}");
    
    // Use EventId for idempotency key in external sink
    await dataLake.UpsertAsync(record.EventId, record.Payload);
    
    // Use Position for durable checkpoint
    await archive.CommitAsync(record.Position);
}
```

Subscriptions commit offsets (stream coordinates), not EventIds. This ensures:
- Crash recovery resumes from the right position
- No gaps or overlaps in subscription progress
- Deterministic replay from any offset

### Deduplication Pattern

Sinks implement idempotency using EventId:

```csharp
public sealed record class DataLakeRecord
{
    public required string EventId { get; init; }  // Deduplication key
    public required DateTimeOffset ReceivedAt { get; init; }
    public required AgentEvent Payload { get; init; }
}

// In Data Lake sink
public async ValueTask UpsertAsync(string eventId, AgentEvent payload)
{
    var record = new DataLakeRecord
    {
        EventId = eventId,
        ReceivedAt = DateTimeOffset.UtcNow,
        Payload = payload
    };
    
    // Upsert by EventId; if duplicate, timestamp will be updated but data is same
    await database.UpsertAsync(
        table: "agent_events",
        key: eventId,
        value: record
    );
}
```

This allows:
- Safe replay after subscriber crash
- Safe retry of failed upstream publishes
- Cross-system correlation (external log has same EventId)

### Stream Position Model

```csharp
public sealed record StreamPosition(
    string Topic,
    int Partition,
    long Offset)
{
    public override string ToString() => $"{Topic}/{Partition}@{Offset}";
}
```

Subscriptions track `StreamPosition`, not EventId, because:
- Offset is monotonic and unique per partition
- Offset survives retention (can be serialized durably)
- EventId may not be globally unique if multiple producers feed the same topic

## Consequences

### Positive

- ✓ Sinks have a stable deduplication key (EventId)
- ✓ Subscriptions have a deterministic replay key (Position)
- ✓ Records are debuggable end-to-end (can correlate via EventId)
- ✓ No information loss (don't have to choose between identities)
- ✓ Matches Kafka/Pulsar conventions (both models are standard)
- ✓ Supports both at-least-once (via sink idempotency) and local exactly-once (via offsets)

### Negative / Trade-offs

- ⚠ **API surface is larger:** Producers must supply EventId
  - *Mitigation:* EventId can be auto-generated if not supplied (GUID), but stable ID is preferred
  
- ⚠ **EventId must be stable:** Producers need discipline to assign meaningful IDs
  - *Mitigation:* Document requirement; code review checklist; warn on random IDs
  
- ⚠ **Storage overhead:** Each record now stores EventId + three coordinates
  - *Mitigation:* Acceptable; typical event is >> 64 bytes of metadata; compression can reduce if needed
  
- ⚠ **Sink responsibility:** Sinks must implement idempotency; no framework handles it
  - *Mitigation:* Provide example processors and sink implementations; document pattern

## Alternatives Considered

1. **EventId only (skip stream coordinates):**
   - Pro: Smaller API, simpler records
   - Con: Sinks cannot deduplicate; subscriptions cannot resume after offset expiration; no replay determinism
   - Decision: Rejected; loses important information

2. **Stream coordinates only (skip EventId):**
   - Pro: Simpler; no producer discipline needed
   - Con: Sinks need external deduplication context; cross-system correlation is hard; no semantic identity
   - Decision: Rejected; loses cross-system debuggability

3. **Automatic GUID-based EventId:**
   - Pro: Simpler for producers
   - Con: Not stable; GUID changes if record is replayed; breaks sink deduplication; defeats the purpose
   - Decision: Rejected; stability is essential

4. **Optional EventId (only if producer supplies it):**
   - Pro: Backward compatible, flexible
   - Con: Sinks cannot rely on EventId; must fallback to coordinates; two code paths
   - Decision: Rejected; required EventId is simpler and forces good design

## Design Notes

### Why Both, Not Just Offset?

In Kafka, offsets are the primary identity. But Kafka's offset model requires:
- Stable leader per partition (elect/rebalance)
- Consumer group coordination (broker-side)
- Cluster replication (offset is durable across brokers)

In a local system, offsets alone are fragile because:
- Retention can delete the offset if subscription lags (OffsetExpired state)
- Replaying the same record from external input may generate a different offset
- Cross-system correlation is lost (external log has different ID)

EventId bridges this gap.

### EventId Generation

**Best practice:** Use stable, meaningful IDs from upstream context:

```csharp
// Good: Stable within upstream system
var eventId = $"scan-job-{scanJobId}";
var eventId = $"agent-{agentId}-{sequenceNumber}";

// Acceptable: Hash of stable inputs
var eventId = ComputeHash($"{source}-{timestamp}-{data}");

// Bad: Random or non-deterministic
var eventId = Guid.NewGuid().ToString();  // Changes on retry
```

### Headers

The `Headers` field allows optional metadata without expanding the fixed record model:

```csharp
record.Headers = new Dictionary<string, string>
{
    ["source"] = "agent-network",
    ["environment"] = "production",
    ["trace-id"] = parentTraceId,
};
```

Sinks can use headers for routing, context, or correlation without modifying the payload.

## Related Decisions

- **ADR-0006:** Delivery semantics (EventId enables at-least-once with deduplication)
- **ADR-0005:** DurableBuffer boundary (stream coordinates are LocalStream-specific)

## Testing

- ✓ Append assigns EventId (auto-generated or supplied)
- ✓ Subscriber reads record with EventId and Position
- ✓ Two records with same EventId at different offsets are both readable (not deduplicated by broker)
- ✓ Sink can deduplicate using EventId
- ✓ Offset can be restored from durable checkpoint
- ✓ Replica replay with same EventId is safe if sink implements upsert

## Notes for Future Maintainers

**If adding optional EventId:**
- Make it required by default
- Only allow null if producer explicitly opts in
- Warn on auto-generated (random) EventIds

**If changing record model:**
- Maintain backward compatibility with old records (offset and EventId immutable once appended)
- Test upgrade path

**For sink implementation examples:**
- Provide sample Data Lake sink with upsert-by-EventId
- Provide sample cache sink with idempotent key
- Provide sample event processor with deduplication
