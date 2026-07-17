# ADR-0010: Processor Model — Local, Not Distributed

**Status:** Accepted

**Date:** 2026-07-17

## Context

Stream processing frameworks span a wide spectrum:

| Framework | Scope | Complexity | Use Case |
|---|---|---|---|
| **Apache Flink** | Distributed, fault-tolerant, stateful | Very high | Large-scale ETL, complex windowing |
| **Kafka Streams** | Co-partitioned with broker, distributed | High | Stream processing at broker scale |
| **Spark Streaming** | Micro-batches, distributed | High | Batch-like stream processing |
| **Node.js Streams** | Local pipes, small libraries | Low | In-process data flow |

LocalStream is a local-only system; it has no need for:
- Distributed processing across nodes
- State replication or checkpointing across hosts
- Partition reassignment or rebalancing
- Fault tolerance across machines

But it does need simple processors for:
- Filtering (logcluster sampling)
- Transformation (silver normalization)
- Deduplication
- Routing to multiple output topics

The risk: **Building a Flink clone** would add significant complexity with zero benefit for local system.

## Decision

**Processors are local, synchronous, single-threaded transformers. No distributed coordination, windowing, or advanced state.**

### Processor API

```csharp
public interface ILocalStreamProcessor<TIn, TOut>
{
    ValueTask ProcessAsync(
        StreamRecord<TIn> input,
        ILocalStreamProducer<TOut> output,
        ProcessorContext context,
        CancellationToken cancellationToken);
}

public sealed record ProcessorContext
{
    public required string TopicName { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required DateTimeOffset RecordUtc { get; init; }
    
    // Commit input offset after processing
    public required Func<ValueTask> CommitAsync { get; init; }
    
    // Simple state access (not distributed)
    public required Func<string, ValueTask<string?>> GetStateAsync { get; init; }
    public required Func<string, string, ValueTask> SetStateAsync { get; init; }
}
```

### Processor Semantics

**Per-record processing:**

```csharp
public class LogClusterSampler : ILocalStreamProcessor<AgentEvent, SampledEvent>
{
    private readonly string _statePath = "./state/sampler";
    
    public async ValueTask ProcessAsync(
        StreamRecord<AgentEvent> input,
        ILocalStreamProducer<SampledEvent> output,
        ProcessorContext context,
        CancellationToken cancellationToken)
    {
        // Transform
        if (input.Payload.ParserStatus != "no_parser")
        {
            var sampled = new SampledEvent
            {
                EventId = input.EventId,
                Source = input.Payload.Source,
                Timestamp = input.PublishedUtc,
                Data = input.Payload.Data,
            };
            
            // Append to output topic
            var result = await output.AppendAsync(
                topic: "logcluster.samples",
                record: sampled,
                options: new AppendOptions { EventId = input.EventId },
                cancellationToken);
            
            if (result.Status != AppendStatus.Appended)
                throw new InvalidOperationException($"Failed to append: {result.Reason}");
        }
        
        // Only commit after output is durable
        await context.CommitAsync();
    }
}
```

**Running a processor:**

```csharp
// Processor runs synchronously, processing one record at a time
// The `RunProcessorOnceAsync` call processes all buffered records
await host.RunProcessorOnceAsync(
    processorName: "logcluster-sampler",
    inputTopic: "agent.output",
    processor: new LogClusterSampler(),
    cancellationToken);
```

Processor framework handles:
- Reading from subscription (durable checkpoint)
- Invoking user processor for each record
- Committing input offset after output is durable
- Managing simple key-value state
- Coordinating with LocalStreamHost lifecycle

### What Processors Are NOT

❌ **Stateful processing** — No windowing, no distributed state
❌ **Scaling** — Each processor runs on one machine
❌ **Rebalancing** — No reassignment of partitions across processors
❌ **Exactly-once guarantees** — Use at-least-once with idempotent output
❌ **Join operations** — No multi-topic stateful joins
❌ **Temporal operations** — No side inputs, no broadcast state

### Processor Patterns

Processors should implement one simple pattern:

1. **Filter + Transform:**
   ```csharp
   if (input matches condition) {
       output.AppendAsync(transformed input);
   }
   await context.CommitAsync();
   ```

2. **Enrich from state:**
   ```csharp
   var enrichment = await context.GetStateAsync(enrichmentKey);
   var enriched = Combine(input, enrichment);
   await output.AppendAsync(enriched);
   await context.CommitAsync();
   ```

3. **Route to multiple outputs:**
   ```csharp
   if (input type A) await topicA.AppendAsync(input);
   if (input type B) await topicB.AppendAsync(input);
   await context.CommitAsync();
   ```

4. **Sample/Deduplicate:**
   ```csharp
   if (ShouldSample(input))
       await output.AppendAsync(input);
   await context.CommitAsync();
   ```

All patterns:
- Read one record
- Optionally write one or more output records
- Commit input offset
- No cross-record state, no windowing

## Consequences

### Positive

- ✓ Simple API (no windowing, no distributed state)
- ✓ Easy to test (pure function + I/O)
- ✓ No rebalancing or failover complexity
- ✓ At-least-once semantics are straightforward
- ✓ Matches DeltaZulu's actual use cases (filter, transform, route)
- ✓ Low learning curve for processor authors
- ✓ Deployment is trivial (single-threaded local processing)

### Negative / Trade-offs

- ⚠ **No advanced windowing:** Cannot compute over time windows
  - *Mitigation:* Rare requirement; batch processing outside LocalStream if needed
  
- ⚠ **No stateful joins:** Cannot correlate events from multiple inputs
  - *Mitigation:* Enrich from external state store, or stage in intermediate topic
  
- ⚠ **No horizontal scaling:** One processor instance per topic
  - *Mitigation:* LocalStream is local-only; scaling happens at application level (multiple partitions, multiple processes)
  
- ⚠ **State is local only:** Not replicated or distributed
  - *Mitigation:* Acceptable for local system; state is backed up by intermediate topics

## Alternatives Considered

1. **Build a full Flink/Spark clone:**
   - Pro: Maximum flexibility
   - Con: Years of engineering; scope creep; most features unused
   - Decision: Rejected; YAGNI

2. **Use Kafka Streams library:**
   - Pro: Battle-tested, full-featured
   - Con: Requires broker; not local-only; overengineered for DeltaZulu
   - Decision: Rejected; wrong model for local system

3. **Support windowing (tumbling, sliding, session):**
   - Pro: More powerful
   - Con: Complex state management; complicates offset semantics; rarely needed
   - Decision: Rejected; defer if actually required

4. **Support joins (stream-stream, stream-table):**
   - Pro: Richer transformations
   - Con: Requires sophisticated state management; partition alignment; rare use case
   - Decision: Rejected; use enrichment from external state store instead

5. **No processors; require external processing:**
   - Pro: Simpler LocalStream implementation
   - Con: Requires separate processing infrastructure; defeats purpose of local broker
   - Decision: Rejected; processors are useful for Agent pipeline

## Design Notes

### Why Synchronous, Per-Record Processing?

In distributed systems, async + batching enables throughput. But in local systems:

- **Throughput isn't bottlenecked by processor** — Archive subscription appending to disk is the bottleneck
- **Simplicity matters** — Async semantics are harder to reason about
- **State is simple** — No need for complex async state machines

Synchronous per-record processing:
- Easier to test (no async ceremony)
- Easier to debug (stack traces are meaningful)
- Natural ordering (input order → output order)

### State is Opportunistic, Not Guarantees

Processor state is durable but not replicated:

```csharp
// State survives process restart
await context.SetStateAsync("config.last-update", DateTime.UtcNow.ToString());
var last = await context.GetStateAsync("config.last-update");

// But if state is lost, processor can recover by replaying input
// (assuming output sink is idempotent, which it should be)
```

This matches at-least-once semantics: output may be duplicated after crash, so input replay is already handled.

### Avoiding Windowing Complexity

Tempting to add windowing:

```csharp
// AVOIDED: Windowed computation
[Window(TimeSpan.FromSeconds(60))]
public async ValueTask ProcessAsync(
    IEnumerable<StreamRecord<AgentEvent>> window,
    ILocalStreamProducer<WindowedResult> output)
{
    var summary = Aggregate(window);
    await output.AppendAsync(summary);
}
```

This requires:
- Buffering records until window closes
- Timer or watermark coordination
- State to track partial windows
- Complex recovery (what if process crashes mid-window?)

Better to use external analytics (Spark, Presto) for windowed aggregations. LocalStream stays local and simple.

## Related Decisions

- **ADR-0005:** DurableBuffer boundary (processors operate on LocalStream topics)
- **ADR-0006:** Delivery semantics (at-least-once, so processor outputs must be idempotent)
- **ADR-0009:** Naming conventions (processor names follow the pattern)

## Testing

- ✓ Processor reads from durable subscription
- ✓ Processor appends to output topic
- ✓ Processor commit only after output is durable
- ✓ Processor state survives restart
- ✓ Duplicate output on crash if input is replayed (at-least-once)
- ✓ Simple filter processor works
- ✓ Multi-output processor works
- ✓ State-based enrichment works

## Notes for Future Maintainers

**If someone asks for windowing:**
- Explain why windowing is excluded (complexity, at-least-once semantics)
- Suggest alternatives:
  - Aggregate to intermediate topic, then batch process
  - Use external analytics (Spark, Presto)
  - Implement application-level windowing (if needed for specific case)

**If someone asks for joins:**
- Explain why joins are excluded (state coordination, partition alignment)
- Suggest alternatives:
  - Enrich from a small state cache/table (load at startup)
  - Stage join inputs in intermediate topics, process together
  - Use external database for correlation

**If processor scalability becomes a bottleneck:**
- Profile first; likely bottleneck is disk, not processor
- Consider: Do you need windowing/state coordination? If no, parallelization via partitions is sufficient
- Defer advanced patterns until measured as necessary

**For SDK users:**
- Keep API surface small
- Document by example (show common patterns)
- Highlight at-least-once requirement for output idempotency
