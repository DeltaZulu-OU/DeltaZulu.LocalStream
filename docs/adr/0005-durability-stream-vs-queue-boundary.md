# ADR-0005: Durability Boundary — LocalStream as Independent Stream Log, Not DurableBuffer Extension

**Status:** Accepted

**Date:** 2026-07-17

## Context

DeltaZulu.DurableBuffer is a simple durable local queue primitive: records are written, sealed into chunks, added to a catalog, dispatched into a bounded channel, then completed, released, or dead-lettered by the consumer.

Early design considered building LocalStream (topics, partitions, subscriptions, retention) on top of or within DurableBuffer. This would mean:

- Reusing DurableBuffer's sealed-chunk lifecycle
- Implementing subscriptions as multiple readers over the same queue
- Using DurableBuffer's delete-on-complete model for retention

**The Problem:** Stream retention is fundamentally different from queue completion.

| Aspect | DurableBuffer (Queue) | LocalStream (Log) |
|---|---|---|
| **Deletion trigger** | Consumer completion | Retention policy (time/size) |
| **Reader independence** | Records deleted when ONE consumer completes | Records retained until policy expires |
| **Subscription state** | Implicit (completion resets) | Explicit (durable offsets) |
| **Multiple subscribers** | Facade over dispatch, not true pub-sub | Independent durable checkpoints per subscriber |

Forcing LocalStream through DurableBuffer's queue model would require:
1. Disabling delete-on-complete to prevent premature record deletion
2. Adding topic/partition routing (complicates queue abstraction)
3. Implementing subscription storage outside the queue (breaks cohesion)
4. Reimplementing retention policy (duplicates DurableBuffer's capability)

This turns DurableBuffer into an embedded broker, violating its design principle of simplicity.

## Decision

**Create LocalStream as an independent library with its own storage layer and lifecycle.**

LocalStream must not reuse DurableBuffer's delete-on-complete queue model. Instead:

### 1. Storage Model Separation

```text
DurableBuffer:
  chunks/
    sealed/
    active/
    deadletter/
    quarantine/

LocalStream:
  topics/
    <topic>/
      partitions/
        <partition>/
          segments/          ← Append-only log segments
            <offset>.log
            <offset>.index
  subscriptions/
    <subscription>/
      <topic>/
        <partition>.checkpoint
```

### 2. Lifecycle Separation

**DurableBuffer:**
- Sealed chunk → Dispatch → Completion/Release/Dead-letter → Delete

**LocalStream:**
- Append to segment → Publish to subscribers → Retention policy evaluation → Automatic delete

### 3. Complementary Use, Not Composition

LocalStream and DurableBuffer work together at **service edges**, not by inheritance:

```text
Agent Output
  → LocalStream append (durable publish)
    → Subscriptions read independently
      → Archive subscription → DurableBuffer edge queue → Data Lake
      → Silver subscription → DurableBuffer edge queue → NRT processor
      → LogCluster subscription → Can drop/sample → DurableBuffer edge queue → sampling
```

This separation allows:
- Archive durability independent of downstream subscriber health
- Optional subscribers (logcluster can drop under pressure)
- Explicit backpressure at each edge

### 4. DurableBuffer Usage Domains (Remains Valid)

DurableBuffer is appropriate for:

- **Edge buffering** — Ingress buffering before LocalStream publish
- **Sink retry buffers** — Buffering output to external systems
- **Dead-letter overflow** — When primary system is unavailable
- **Processor output buffering** — Staging processor results

LocalStream is appropriate for:

- **Topic append** — Primary publish
- **Subscription reads** — Replay and streaming
- **Retention enforcement** — Policy-based cleanup
- **Offset management** — Durable subscription state

## Consequences

### Positive

- ✓ LocalStream retains full control of retention policy
- ✓ Independent subscription checkpoints are native, not bolted on
- ✓ DurableBuffer remains simple (no embedded broker complexity)
- ✓ Clear separation of concerns (queue vs. log)
- ✓ Easier to reason about crash recovery (separate lifecycles)
- ✓ Each library optimized for its use case

### Negative / Trade-offs

- ⚠ **Code duplication:** Some features may be implemented twice (checksums, recovery, metrics)
  - *Mitigation:* Acceptable for clarity; shared utilities can be extracted (e.g., file format constants)
  
- ⚠ **Disk footprint:** Two separate storage systems instead of one unified queue
  - *Mitigation:* Acceptable for local-only system; disk is cheap at local scale
  
- ⚠ **Operational complexity:** Operators manage two separate stores
  - *Mitigation:* Clear documentation and unified configuration bridge the gap

## Alternatives Considered

1. **Extend DurableBuffer with pub-sub:**
   - Pro: Single storage layer
   - Con: Violates DurableBuffer's simplicity; queue lifecycle doesn't match stream needs
   - Decision: Rejected; creates scope creep

2. **Implement LocalStream as a thin wrapper over DurableBuffer:**
   - Pro: Reuses tested code
   - Con: Must disable or work around queue lifecycle; confusion about which features work
   - Decision: Rejected; introduces hidden coupling

3. **Use a hybrid approach: Queue for recent data, stream log for archival:**
   - Pro: Balances performance and simplicity
   - Con: Adds complexity (dual write paths, rebalancing logic); doesn't solve the fundamental mismatch
   - Decision: Rejected; cleaner to keep them separate

4. **Single library with pluggable backends:**
   - Pro: Unified API
   - Con: Abstracts away important differences; each backend needs different tuning
   - Decision: Rejected; premature abstraction (YAGNI)

## Related Decisions

- **ADR-0000:** Overall optimization philosophy (keep LocalStream independent for performance tuning)
- **ADR-0006:** Delivery semantics (which are specific to LocalStream's log model)

## Notes for Future Maintainers

**If considering merging LocalStream and DurableBuffer:**
- Ask: Does the queue model match the retention needs? (Answer: No)
- Ask: Can subscription checkpoints be added to queue completion? (Answer: Would require disabling queue lifecycle)
- Conclusion: Keep separate. Merge only if requirements fundamentally change.

**If adding DurableBuffer edge buffering to LocalStream subscriptions:**
- Treat it as an optional integration, not a replacement
- Document the configuration clearly
- Test crash recovery scenarios involving both stores

**Versioning:** DurableBuffer and LocalStream should version independently. They may have different release cycles.

## Testing

- ✓ LocalStream tests do not depend on DurableBuffer APIs
- ✓ DurableBuffer tests do not depend on LocalStream APIs
- ✓ Integration tests verify edge buffering scenarios work correctly
- ✓ Retention policy enforcement is independent of subscription progress
