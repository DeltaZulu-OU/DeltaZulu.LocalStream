# ADR-0006: Delivery Semantics — At-Least-Once, Not Exactly-Once

**Status:** Accepted

**Date:** 2026-07-17

## Context

Stream processors and brokers (Kafka, Pulsar, Flink) support three delivery guarantees:

1. **At-most-once** — Record delivered 0 or 1 times; may drop under failure
2. **At-least-once** — Record delivered 1+ times; may duplicate after crash
3. **Exactly-once** — Record delivered exactly once; duplicates are impossible

Exactly-once requires:

- Distributed consensus on offset commits
- Transactional writes (append + commit atomic)
- Two-phase commit or equivalent coordination
- Recovery protocol to prevent duplicate replay

For a **local-only, non-distributed system**, exactly-once introduces complexity with minimal benefit:

| Scenario | Risk | Mitigation |
|---|---|---|
| **Crash during append** | Record may be lost | N/A — already handled by durability strategy (CRC, index) |
| **Crash after append, before offset commit** | Subscriber replays record | Application must be idempotent anyway |
| **Crash in middle of processor output + input commit** | Duplicate output | Processor output sinks must use deduplication key (EventId or stream coordinates) |

In a distributed system, duplicates from crash recovery are rare (require specific crash timing). In a local system where restarting is fast, at-least-once with idempotent processors is pragmatic.

## Decision

**Claim at-least-once delivery semantics. Do not claim exactly-once.**

### Producer Append Behavior

```csharp
public sealed record AppendResult
{
    public required AppendStatus Status { get; init; }
    public StreamPosition? Position { get; init; }  // Topic, Partition, Offset
    public string? EventId { get; init; }
    public string? Reason { get; init; }
}

public enum AppendStatus
{
    Appended,                    // ← At-least-once: now durable
    RejectedTopicNotFound,
    RejectedStreamFull,
    RejectedRecordTooLarge,
    FailedSerialization,
    FailedStorage,
    Cancelled
}
```

From the producer's perspective, an append is successful only after the record is durable on disk. If the append returns `Appended`, the record may be replayed by the subscription after a crash, but will not be lost.

### Subscription Read Behavior

```csharp
var record = await subscription.ReadAsync("agent.output").FirstAsync();
await subscription.CommitAsync(record.Position);  // Offset durable now
```

If the process crashes **before** commit, the same record will be re-delivered after restart. This is expected at-least-once behavior.

If the process crashes **after** commit but **before** the external side effect completes, the subscriber must handle the duplicate via:
- `EventId` — stable cross-system identity
- Stream coordinates — Topic + Partition + Offset
- Idempotency keys in sink (database upsert, cache key, etc.)

### Processor Behavior

```csharp
public async ValueTask ProcessAsync(
    StreamRecord<TIn> input,
    ILocalStreamProducer<TOut> output,
    ProcessorContext context,
    CancellationToken cancellationToken)
{
    // Append output first
    var result = await output.AppendAsync("output.topic", transformedRecord, ...);
    
    // Commit input offset only after output is durable
    await context.CommitAsync(input.Position);
}
```

If a process crashes between append and commit:
1. Output record is durable (was appended successfully)
2. Input offset is not committed
3. Processor restarts and replays the same input
4. Same output record is appended again (duplicate)

The output sink must deduplicate using EventId or stream coordinates.

## Consequences

### Positive

- ✓ Simple local lifecycle (no distributed consensus)
- ✓ Fast crash recovery (no coordination overhead)
- ✓ Honest guarantees (no false exactly-once claims)
- ✓ Defers complexity to sinks (idempotency is sink responsibility)
- ✓ Allows optional subscribers to drop under pressure without correctness impact
- ✓ Matches Kafka/Pulsar defaults (at-least-once is standard)

### Negative / Trade-offs

- ⚠ **Subscribers may see duplicates:** Essential to design sinks for idempotency
  - *Mitigation:* Document requirement clearly in API. Provide `EventId` and `StreamPosition` as deduplication keys
  
- ⚠ **No protection against logic errors:** If a sink forgets idempotency, duplicates will propagate
  - *Mitigation:* Code review checklist. Example processors in docs.
  
- ⚠ **Processor state may be partially replayed:** If processor state is durable, replaying input may skip state updates
  - *Mitigation:* Keep processor state logic simple. Avoid stateful computations; prefer pure functions.

## Alternatives Considered

1. **Claim exactly-once with distributed consensus:**
   - Pro: Eliminates duplicate handling in sinks
   - Con: Adds complexity (2PC, consensus protocol, recovery coordination); overkill for local system
   - Decision: Rejected; not worth the complexity

2. **Hybrid approach: At-least-once with transactional append + commit:**
   - Pro: Reduces crashes between append and commit
   - Con: Only helps for crashes before commit; sinks still need idempotency for multi-step outputs; minor benefit
   - Decision: Rejected; added complexity, minimal benefit

3. **Provide both at-least-once and exactly-once modes:**
   - Pro: Flexibility
   - Con: Doubles test surface; confusing API; most users don't need exactly-once
   - Decision: Rejected; premature complexity (YAGNI)

4. **Promise at-least-once via replay window, not per-crash:**
   - Pro: Reduces perceived uncertainty
   - Con: Still requires sinks to be idempotent; doesn't change the challenge
   - Decision: Rejected; doesn't solve the underlying issue

## Design Notes

### Why Not "Exactly-Once"?

- **Exactly-once is a lie in local systems:** Even with transactional writes, process crash + restart + subscription re-read = replay. Claiming exactly-once overstates what's possible without distributed coordination.

- **Sinks already need idempotency:** Data Lake, NRT Silver, and LogCluster all write to external systems (HDFS, databases, caches). They must deduplicate using EventId or coordinates. At-least-once doesn't add burden; it's already present.

- **Honest semantics build confidence:** "At-least-once with idempotent sinks" is clear. Sink owners know what to implement.

### Message Ordering

Ordering **is** preserved within a partition:

```text
Partition 0: [Record A] → [Record B] → [Record C]
Subscription: Always reads A before B before C
Commit offset after B: Restart returns from C onward
```

Partition-level ordering is sufficient for most stream use cases. Cross-partition ordering is not guaranteed (nor claimed).

## Related Decisions

- **ADR-0005:** DurableBuffer vs LocalStream boundary (LocalStream's at-least-once model requires independent subscriptions)
- **ADR-0000:** Philosophy (accept crash-recovery trade-offs)

## Testing

- ✓ Append succeeds → record is durable
- ✓ Crash before commit → same record delivered after restart
- ✓ Crash after commit → record not replayed
- ✓ Processor output appended before input commit → allows replay detection in sinks
- ✓ Partition ordering is preserved across restart

## Notes for Future Maintainers

**If a consumer reports duplicates after crash:**
- This is expected. Verify:
  1. The crash occurred between read and commit
  2. The sink implemented idempotency (or should have)
  3. Offset was persisted correctly

**If considering a move to exactly-once:**
- Measure first: How many duplicates are actually occurring in production?
- Evaluate cost: What's the complexity and performance cost of adding consensus?
- Decide: Is the ROI worth it? (Probably not for local system.)

**Documentation for end users:**
- Clearly state: "At-least-once delivery. Sinks must be idempotent."
- Provide example processor with idempotent output logic
- Explain EventId and stream coordinates
