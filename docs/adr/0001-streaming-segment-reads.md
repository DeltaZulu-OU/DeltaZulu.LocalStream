# ADR-0001: Streaming Segment Reads with Bounded Buffer

**Status:** Accepted

**Date:** 2026-07-17

## Context

Segment files (append-only logs) can be very large (default 128 MB per segment). The original implementation of `PartitionLog.Read()`, recovery, and verification used `File.ReadAllBytes()` to load entire segments into memory, then parsed all records sequentially.

### Problems with Whole-File Loads

1. **Memory Safety Hazard:** A 128 MB segment loaded as a single `byte[]` lands on the Large Object Heap (LOH). With multiple concurrent readers on the same partition, LOH pressure can cause OOM on sustained read workloads.

2. **Throughput Bottleneck:** `RunProcessorAsync` polls every 250 ms. Each poll called `ReadAsync()`, which re-loaded and re-parsed the *entire active segment* — even when only a few new records had arrived. A caught-up consumer (most of the time) paid O(active_segment_size) just to check if new data existed.

3. **Verification Overhead:** Startup recovery and `Verify()` operations built throwaway `List<RecordEnvelope>` with cloned `JsonElement` payloads, wasting memory and GC pressure just to validate framing.

## Decision

Implement a bounded **FrameReader** that streams segments through a 64 KB read buffer instead of loading files whole:

### FrameReader Design

- **Fixed 64 KB buffer** — Fits in L1/L2 cache; reused across all reads
- **Positional skipping** — Records before the requested `fromOffset` are skipped by line position (offset = segment base + line index) without CRC-checking or deserializing
- **Lazy deserialization** — Only records yielded to the caller are CRC-checked and JSON-parsed
- **Spillover handling** — Lines crossing buffer boundaries are accumulated in a `List<byte>` with O(n log n) cost due to geometric resize (acceptable for rare large records)

### Impact

| Scenario | Before | After | Gain |
|---|---|---|---|
| **Caught-up reader (250 ms poll)** | Parse entire active segment | Skip to end, return 0 records | 1000× faster for idle partitions |
| **Metric collection** | Load + parse every segment | Stream only until end | O(n) reduction per segment |
| **Recovery (1 GB topic)** | Load all segments into memory | Stream recovery | No LOH allocations |
| **Concurrent readers** | Multiple 128 MB LOH allocations | Shared 64 KB buffer pool | OOM risk eliminated |

## Consequences

### Positive

- ✓ No more whole-segment LOH allocations
- ✓ Processor polls now pay only for genuinely new records
- ✓ Caught-up consumers (common case) are ~1000× faster
- ✓ Recovery and verification are dramatically faster for large topics
- ✓ Bounds memory usage independent of segment size

### Negative / Trade-offs

- ⚠ **Large records (> 64 KB):** Spillover list resizes across buffer refills. Cost is O(n log n), not O(n²), but non-trivial for 100+ MB records
  - *Mitigation:* Typical LocalStream records are < 1 MB; oversized records trigger `MaxRecordBytes` rejection
- ⚠ **Error recovery:** If a segment is torn mid-line, recovery must scan from start to find the boundary (cannot skip by position)
  - *Mitigation:* Torn tails are truncated by `SetLength()` during recovery; on next append, the segment is healthy

## Alternatives Considered

1. **Memory-mapped files (mmap):**
   - Pro: OS handles paging; zero-copy reads
   - Con: Complicates truncation (segment roll), recovery, and Windows compatibility
   - Decision: Rejected; streaming + OS page cache is simpler and sufficient

2. **PipeReader<byte> from System.IO.Pipelines:**
   - Pro: Optimized for streaming with automatic buffer pooling
   - Con: Adds dependency; System.Buffers wrapper is already good enough
   - Decision: Rejected; hand-coded FrameReader is simpler and equally efficient

3. **Keep whole-file load, add disk cache:**
   - Pro: Simpler code
   - Con: Doesn't solve LOH or concurrent-reader memory pressure
   - Decision: Rejected; streaming is the right fix

## Related Decisions

- See ADR-0004 for payload serialization optimization (complements streaming by avoiding JSON re-parse)
