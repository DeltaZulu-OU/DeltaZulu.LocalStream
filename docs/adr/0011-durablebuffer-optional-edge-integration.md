# ADR-0011: DurableBuffer as Optional Edge Integration, Not LocalStream Dependency

**Status:** Accepted

**Date:** 2026-07-21

## Context

Earlier LocalStream revisions carried a direct repository and project-level connection to `DeltaZulu.DurableBuffer` so the stream library could reuse the durable queue primitive. That made fresh checkouts and builds depend on the DurableBuffer submodule and blurred the architectural boundary between a queue and a stream log.

The current implementation no longer has a compile-time dependency on DurableBuffer. LocalStream owns its append-only topic segments, offset assignment, subscription checkpoint files, retention evaluation, and recovery path.

This creates an explicit question for maintainers: did the removal accidentally duplicate useful DurableBuffer functionality, and should the dependency be reintroduced?

## Decision

**Keep `DeltaZulu.LocalStream` independent from `DeltaZulu.DurableBuffer` at compile time.**

DurableBuffer remains a sibling library for local queue and spool use cases, but it is not part of LocalStream's core storage engine and should not be required to build, test, or package LocalStream.

LocalStream may integrate with DurableBuffer at service edges, where queue semantics are the desired behavior:

```text
Producer / ingress retry
  → optional DurableBuffer ingress queue
  → LocalStream topic append
  → independent LocalStream subscriptions
    → optional DurableBuffer sink retry queue
    → external sink / processor / archive
```

LocalStream should continue to own these stream-log responsibilities directly:

- topic and partition routing
- monotonic per-partition offsets
- append-only segment storage
- durable subscription checkpoints
- replay from retained offsets
- retention by time and size policy
- offset expiration reporting
- processor input commits after durable output

DurableBuffer should continue to own these queue/spool responsibilities when a caller opts into it:

- bounded producer/consumer buffering
- dispatch channel backpressure
- in-flight queue accounting
- completion, release, dead-letter, and quarantine flows
- retry staging for external systems

## Duplication Assessment

Some low-level durability techniques are duplicated intentionally:

| Concern | LocalStream need | DurableBuffer need | Decision |
|---|---|---|---|
| File safety | Protect stream segments and checkpoints | Protect queue chunks and catalogs | Duplicate or later extract small utilities |
| Checksums | Validate segment frames during reads/recovery | Validate buffered chunks | Duplicate unless a shared format emerges |
| Recovery | Rebuild stream offsets and segment state | Rebuild queue catalog/chunk state | Keep separate because lifecycle differs |
| Metrics | Topic, partition, lag, retention, subscription state | Queue depth, in-flight chunks, dispatch wait reason | Keep separate because operational questions differ |
| Retention/cleanup | Policy-based deletion independent of subscribers | Completion/dead-letter/quarantine lifecycle | Must remain separate |

The duplication is acceptable because these libraries optimize different invariants. A shared dependency would be justified only for small, semantics-free helpers, such as atomic file writes, CRC implementations, file permission helpers, or path hardening utilities. Shared helpers must not introduce queue lifecycle concepts into LocalStream.

## Consequences

### Positive

- LocalStream can be cloned, restored, built, tested, and packaged without initializing a submodule.
- The stream log remains free from DurableBuffer's delete-on-complete lifecycle.
- Subscription checkpoints and retention policy stay native to LocalStream instead of being layered around a queue.
- DurableBuffer remains focused on local queue/spool behavior instead of becoming an embedded broker.
- Both libraries can version independently.

### Negative / Trade-offs

- Low-level file durability code may be repeated.
- Operators may use both libraries in one service and must configure two stores when edge queues are enabled.
- Documentation must be clear that DurableBuffer is optional and not included as a required LocalStream dependency.

## Alternatives Considered

1. **Reintroduce DurableBuffer as a required project or submodule dependency**
   - Pro: Reuses existing queue implementation and tests.
   - Con: Reintroduces checkout/build coupling and tempts LocalStream to inherit queue lifecycle semantics.
   - Decision: Rejected.

2. **Build LocalStream as a thin wrapper over DurableBuffer**
   - Pro: Smaller LocalStream storage implementation.
   - Con: Requires disabling or bypassing delete-on-complete, bolting on subscription checkpoints, and reimplementing stream retention outside the queue.
   - Decision: Rejected.

3. **Extract a shared `DeltaZulu.StoragePrimitives` package**
   - Pro: Reduces duplicated CRC, atomic write, and file hardening code.
   - Con: Premature until duplication becomes costly and stable enough to standardize.
   - Decision: Deferred. Acceptable later if it contains only lifecycle-neutral utilities.

4. **Keep DurableBuffer as optional edge integration**
   - Pro: Preserves the queue/log boundary while allowing reliable ingress and sink retry flows.
   - Con: Requires explicit integration code where services need both libraries.
   - Decision: Accepted.

## Guidance for Future Maintainers

Do not reintroduce a required DurableBuffer dependency merely to avoid duplicated file I/O, checksum, recovery, or metrics code. Reconsider only if LocalStream's core requirements change from stream-log semantics to queue semantics.

If adding a DurableBuffer integration package or sample:

- keep it outside the core `DeltaZulu.LocalStream` assembly;
- document that it is optional;
- ensure LocalStream tests still pass without DurableBuffer present;
- test crash recovery across both stores when edge buffering is enabled.

If extracting shared primitives:

- keep the shared package free of queue, subscription, topic, and retention policy concepts;
- preserve independent versioning for LocalStream and DurableBuffer;
- migrate only stable primitives with clear ownership.

## Related Decisions

- **ADR-0005:** DurableBuffer boundary; LocalStream is an independent stream log, not a DurableBuffer extension.
- **ADR-0006:** At-least-once delivery; processor commits rely on LocalStream offsets.
- **ADR-0008:** Retention independence; retention is policy-based rather than completion-based.
- **ADR-0010:** Processor model; processors operate locally over LocalStream subscriptions.
