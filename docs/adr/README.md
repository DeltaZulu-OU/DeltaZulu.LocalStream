# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records for DeltaZulu.LocalStream, documenting important design choices, performance optimizations, and trade-offs.

## Purpose

ADRs prevent regressions and maintain institutional knowledge about *why* code is written a certain way. They're not rules; they're reasoned decisions that can be revisited if context changes.

## Quick Navigation

| ADR | Title | Topic | Status |
|---|---|---|---|
| [0000](0000-optimization-philosophy.md) | Design Philosophy | Overall optimization principles | Accepted |
| [0001](0001-streaming-segment-reads.md) | Streaming Segment Reads | Eliminate LOH allocations, bounds buffer usage | Accepted |
| [0002](0002-retention-complexity-optimization.md) | Retention Complexity | Fix O(n²) Sum loop → O(n) | Accepted |
| [0003](0003-event-driven-memoization-for-metrics.md) | Event-Driven Memoization | Cache metrics with explicit invalidation | Accepted |
| [0004](0004-payload-serialization-without-round-trip.md) | Payload Serialization | Avoid JSON re-parse and clone | Accepted |
| [0005](0005-durability-stream-vs-queue-boundary.md) | DurableBuffer Boundary | LocalStream vs DurableBuffer separation | Accepted |
| [0006](0006-delivery-semantics-at-least-once.md) | Delivery Semantics | At-least-once, not exactly-once | Accepted |
| [0007](0007-record-identity-stream-coordinates.md) | Record Identity | EventId and stream coordinates | Accepted |
| [0008](0008-retention-independence.md) | Retention Independence | Retention by policy, not subscription progress | Accepted |
| [0009](0009-naming-conventions.md) | Naming Conventions | Topic, subscription, processor naming | Accepted |
| [0010](0010-processor-model-local-scope.md) | Processor Model | Local processors, not distributed | Accepted |

## Reading Guide

**New to the codebase?**
- Start with [0000-optimization-philosophy.md](0000-optimization-philosophy.md) — it explains the overall philosophy and decision matrix

**Interested in a specific area?**

*Performance optimizations:*
- Performance concern? → [0000](0000-optimization-philosophy.md) (decision matrix)
- Memory pressure? → [0001](0001-streaming-segment-reads.md)
- Retention slow? → [0002](0002-retention-complexity-optimization.md)
- Metrics expensive? → [0003](0003-event-driven-memoization-for-metrics.md)
- Append throughput? → [0004](0004-payload-serialization-without-round-trip.md)

*Architecture and design:*
- Library boundary decisions? → [0005](0005-durability-stream-vs-queue-boundary.md)
- Delivery guarantees? → [0006](0006-delivery-semantics-at-least-once.md)
- Record structure? → [0007](0007-record-identity-stream-coordinates.md)
- Retention policy? → [0008](0008-retention-independence.md)
- Naming topics/subscriptions? → [0009](0009-naming-conventions.md)
- Building a processor? → [0010](0010-processor-model-local-scope.md)

**Maintenance task?**
- See ADR-0000's "Review Checklist for Future Optimizations" before adding new optimizations

## Key Themes

### Algorithmic Efficiency Over Micro-Optimizations

All optimizations target:
1. **Eliminating O(n²)** — Delete segments in retention (ADR-0002)
2. **Reducing redundant O(n)** — Recomputing metrics every call (ADR-0003)
3. **Removing wasteful serialization** — JSON parse → clone → re-serialize (ADR-0004)
4. **Bounding memory** — Segment reads stay under 64 KB (ADR-0001)

### Event-Driven Invalidation, Not TTL

Caches are tied to operations that change state (retention, recovery), not arbitrary time windows. This gives deterministic, debuggable behavior.

### Trade-offs Are Documented

Each ADR explicitly states:
- What you gain (performance, safety, clarity)
- What you give up (complexity, crash-recovery scenarios)
- Why the trade-off is acceptable in this context

## Reverting or Updating an ADR

If an optimization becomes problematic:

1. **Open an issue** describing the problem (e.g., "streaming reads cause GC spikes")
2. **Create a new ADR** (e.g., ADR-0005) with the new decision
3. **Reference the old ADR** in the new one (e.g., "supersedes ADR-0001")
4. **Update this README** to point to the new ADR

Example:
```markdown
| [0005](0005-mmap-for-large-segments.md) | Memory-Mapped Reads | Supersedes ADR-0001 for segments > 256 MB | Accepted |
```

## Format

All ADRs follow the template:
```markdown
# ADR-XXXX: Title

**Status:** Accepted | Proposed | Superseded

**Date:** YYYY-MM-DD

## Context
(Why is this decision needed?)

## Decision
(What is the decision?)

## Consequences
(What are the benefits and trade-offs?)

## Alternatives Considered
(What else was evaluated?)

## Related Decisions
(Cross-references to other ADRs)
```

See [Michael Nygard's ADR template](https://github.com/joelparkerhenderson/architecture_decision_record) for reference.

## ADR Workflow

1. **Identify** an architectural decision or significant optimization
2. **Propose** with a new ADR document (use next available number)
3. **Review** with team; get buy-in
4. **Accept** and merge
5. **Implement** the decision
6. **Maintain** — keep the ADR accurate as the system evolves

## ADR Categories

**Performance & Implementation (0001–0004):**
Optimize hot paths and eliminate wasteful operations without changing the architecture. See [ADR-0000](0000-optimization-philosophy.md) for the decision matrix.

**Architecture & Design (0005–0010):**
Decisions about library boundaries, delivery guarantees, record structure, retention, naming, and processor scope. These are strategic choices that rarely change.

## Related Reading

- [LOCAL_STREAM_ARCHITECTURE.md](../LOCAL_STREAM_ARCHITECTURE.md) — Overall system design (historical context; architectural decisions now formalized as ADRs 0005–0010)
- [README.md](../../README.md) — Usage guide and quick start
- Git commit messages — Implementation details (grep for commit author and date in ADRs)
