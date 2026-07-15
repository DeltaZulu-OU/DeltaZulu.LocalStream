# DeltaZulu.LocalStream Architecture

## Status

Proposed architecture for a local, Kafka/Pulsar/Flink-inspired stream runtime used by DeltaZulu Platform and related services.

This document defines a separate library named **DeltaZulu.LocalStream**. It is intentionally not implemented inside **DeltaZulu.DurableBuffer**. DurableBuffer remains the simple durable local queue primitive. LocalStream builds higher-level topic, partition, offset, subscription, retention, replay, and processor semantics on top of a local append-only stream model.

## Naming Decision

The recommended library name is:

```text
DeltaZulu.LocalStream
```

The name is preferred over `DeltaZulu.DurablePubSub` because the desired system is broader than pub-sub. It needs Kafka-like topics and offsets, Pulsar-like subscriptions, and eventually a small Flink-like processing model. “PubSub” describes only one delivery pattern. “LocalStream” describes the actual abstraction: a local durable stream log with named consumers and replay.

Package and namespace names:

```text
src/DeltaZulu.LocalStream/
tests/DeltaZulu.LocalStream.Tests/
docs/LOCAL_STREAM_ARCHITECTURE.md

namespace DeltaZulu.LocalStream
```

Primary types:

```text
LocalStreamHost
ILocalStreamProducer<T>
ILocalStreamConsumer<T>
ILocalStreamProcessor<TIn,TOut>
StreamTopic
StreamPartition
StreamOffset
StreamPosition
StreamRecord<T>
StreamSubscription
StreamCheckpoint
StreamProcessorContext
```

Avoid these names for the core library:

```text
DeltaZulu.DurablePubSub
DeltaZulu.LocalBroker
DeltaZulu.EmbeddedKafka
DeltaZulu.StreamBroker
```

`DurablePubSub` may be used later as a compatibility facade over LocalStream, but it should not be the primary architectural name.

## Background

`DeltaZulu.DurableBuffer` is already a good local queue primitive. It provides durable disk-backed buffering between a producer pipeline and an application-owned consumer, with sealed chunks, bounded disk and memory usage, backpressure, recovery, metrics, lifecycle events, dead-lettering, and quarantine storage. It is intentionally small and format-agnostic.

The current DurableBuffer architecture deliberately keeps the core contract narrow. It is a single-buffer primitive managing one sealed-chunk queue. It does not own network forwarding or retry policy, and it exposes chunks to application code for completion, release, or dead-lettering.

The updated documentation also correctly states that DurableBuffer’s Rx surface is a facade over queue dispatch, not broadcast pub-sub. It does not add broker, topic, fan-out, or per-subscriber durable state.

That makes DurableBuffer the wrong place to add Kafka/Pulsar/Flink-style semantics. Those semantics belong in a separate library.

## Problem Statement

DeltaZulu.Platform needs to publish output events to multiple logical consumers:

```text
archive
silver / NRT normalization
logcluster sample pipeline
diagnostic or local debug consumers
```

These consumers should not compete for records. They should independently read the same event stream and commit their own progress. The desired model is similar to Kafka, Pulsar, or Flink, but local-only and non-distributed for now.

The system does not currently require:

```text
distributed brokers
replicated partitions
leader election
remote consumer groups
cluster membership
cross-node exactly-once guarantees
```

The system does require:

```text
append once
read many times
independent subscriptions
offset commits
local replay
bounded retention
local backpressure visibility
processor checkpoints
stable event identity
```

## Architectural Decision

Create a new library:

```text
DeltaZulu.LocalStream
```

`DeltaZulu.LocalStream` is a local durable stream runtime. It stores records in append-only topic partitions and lets named subscriptions read independently using durable offsets.

The relationship is:

```text
DeltaZulu.DurableBuffer
  = simple durable local queue / spool primitive

DeltaZulu.LocalStream
  = local stream log with topics, partitions, offsets, subscriptions, replay, retention, and processors
```

DurableBuffer may still be used at service edges or as an implementation utility, but LocalStream must not be forced through DurableBuffer’s delete-on-complete queue lifecycle. Kafka-like semantics require retention independent of consumer completion.

## Goals

DeltaZulu.LocalStream must provide:

```text
local topic abstraction
partitioned append-only logs
monotonic offsets per partition
stable stream coordinates
named subscriptions
durable subscription checkpoints
replay from offset, timestamp, earliest, latest, or committed
policy-based retention
per-subscription lag metrics
at-least-once delivery
idempotency support through stable event IDs
local processor model
```

It must support the DeltaZulu.Platform output pipeline without requiring an external broker.

## Non-Goals

DeltaZulu.LocalStream is not intended to provide:

```text
distributed queues
cluster replication
remote broker API compatibility
Kafka protocol compatibility
Pulsar protocol compatibility
Flink runtime compatibility
exactly-once distributed processing
multi-node partition ownership
cross-host consumer groups
```

The naming should not imply that this is Kafka-compatible. It is Kafka-inspired, not Kafka-compatible.

## Conceptual Model

The central abstraction is a local stream log.

```text
Producer
  -> Topic
      -> Partition 0 append-only segment log
      -> Partition 1 append-only segment log
      -> Partition N append-only segment log

Subscriptions
  -> archive subscription offset
  -> silver subscription offset
  -> logcluster subscription offset
```

Each topic contains one or more partitions. Each partition is an ordered append-only sequence of records. Every record receives a position:

```text
topic + partition + offset
```

Each subscription stores its own committed offsets. Reading a record does not delete it. Retention deletes old data by size/time policy, not by consumer completion.

## Core Components

### LocalStreamHost

`LocalStreamHost` owns the runtime lifecycle.

Responsibilities:

```text
load topic metadata
recover partition segments
recover subscription checkpoints
start retention workers
start processor workers if configured
expose producer and consumer APIs
publish runtime metrics
stop cleanly
```

### Topic Store

The topic store owns append-only segment files and indexes.

Responsibilities:

```text
append records
assign offsets
maintain segment indexes
validate segment checksums
recover segments on startup
enforce retention policy
support reads from offset or timestamp
```

### Subscription Store

The subscription store owns named reader state.

Responsibilities:

```text
register subscriptions
persist committed offsets
recover checkpoints
track lag
support reset/replay
support pause/resume
```

### Processor Runtime

The processor runtime is a small local stream-processing layer.

Responsibilities:

```text
consume input topic records
apply filter/map/sample/normalize logic
write output records to another topic or external sink
commit offsets after successful processing
store lightweight processor state
```

This should not try to become Flink. It should only implement the subset DeltaZulu needs locally.

## Storage Layout

Recommended V1 layout:

```text
localstream/
  metadata/
    topics.json
    subscriptions.json

  topics/
    agent.output/
      partitions/
        000000/
          segments/
            00000000000000000000.log
            00000000000000000000.index
            00000000000000100000.log
            00000000000000100000.index
          partition.state.json

        000001/
          segments/
          partition.state.json

  subscriptions/
    archive/
      agent.output/
        000000.checkpoint
        000001.checkpoint

    silver/
      agent.output/
        000000.checkpoint
        000001.checkpoint

    logcluster/
      agent.output/
        000000.checkpoint
        000001.checkpoint

  processors/
    silver-normalizer/
      state/
      checkpoints/

    logcluster-sampler/
      state/
      checkpoints/

  quarantine/
  deadletter/
```

This storage model differs from DurableBuffer. DurableBuffer stores active, sealed, deadletter, and quarantine chunks for queue-style consumption. LocalStream stores append-only topic segments and subscription checkpoints.

## Record Model

A stream record should contain immutable stream coordinates and stable event identity.

```csharp
public sealed record StreamRecord<T>
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string EventId { get; init; }
    public required DateTimeOffset PublishedUtc { get; init; }
    public required IReadOnlyDictionary<string, string> Headers { get; init; }
    public required T Payload { get; init; }
}
```

`EventId` is the cross-system identity. `Topic`, `Partition`, and `Offset` are stream coordinates. Sinks should use `EventId` or stream coordinates for idempotency.

## Topic Naming

Use dot-separated topic names.

Recommended topics:

```text
agent.output
agent.parser-status
agent.deadletter
silver.process-events
silver.network-sessions
silver.authentications
logcluster.samples
logcluster.candidates
```

Rules:

```text
lowercase
dot-separated
domain first
purpose second
plural only when the topic contains typed entities
no transport names in topic names
no environment names in topic names
```

Good:

```text
agent.output
logcluster.samples
silver.process-events
```

Bad:

```text
KafkaAgentOutput
agentOutputQueue
prod-agent-output
relp-output
```

## Subscription Naming

Use stable lowercase names.

Recommended subscriptions:

```text
archive
silver
logcluster
debug-local
```

If a subscription belongs to a processor, use:

```text
processor.<processor-name>
```

Examples:

```text
processor.silver-normalizer
processor.logcluster-sampler
```

## API Design

### Producer API

```csharp
public interface ILocalStreamProducer<T>
{
    ValueTask<AppendResult> AppendAsync(
        string topic,
        T record,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask FlushAsync(
        string topic,
        CancellationToken cancellationToken = default);
}
```

### Consumer API

```csharp
public interface ILocalStreamConsumer<T>
{
    string SubscriptionId { get; }

    IAsyncEnumerable<StreamRecord<T>> ReadAsync(
        string topic,
        ReadOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask CommitAsync(
        StreamPosition position,
        CancellationToken cancellationToken = default);

    ValueTask ResetAsync(
        string topic,
        ResetPosition position,
        CancellationToken cancellationToken = default);
}
```

### Position Model

```csharp
public sealed record StreamPosition(
    string Topic,
    int Partition,
    long Offset);
```

### Append Result

```csharp
public sealed record AppendResult
{
    public required AppendStatus Status { get; init; }
    public StreamPosition? Position { get; init; }
    public string? EventId { get; init; }
    public string? Reason { get; init; }
}
```

```csharp
public enum AppendStatus
{
    Appended,
    RejectedTopicNotFound,
    RejectedStreamFull,
    RejectedRecordTooLarge,
    FailedSerialization,
    FailedStorage,
    Cancelled
}
```

### Processor API

```csharp
public interface ILocalStreamProcessor<TIn, TOut>
{
    ValueTask ProcessAsync(
        StreamRecord<TIn> input,
        ILocalStreamProducer<TOut> output,
        ProcessorContext context,
        CancellationToken cancellationToken = default);
}
```

## Delivery Semantics

DeltaZulu.LocalStream should make conservative delivery claims.

| Concern         | Semantics                                                 |
| --------------- | --------------------------------------------------------- |
| Producer append | At-least-once from caller perspective                     |
| Consumer read   | At-least-once                                             |
| Offset commit   | Durable per subscription and partition                    |
| Ordering        | Preserved within one partition                            |
| Replay          | Supported while retained                                  |
| Deletion        | Retention-based, not consumer-completion-based            |
| Exactly once    | Not claimed                                               |
| Idempotency     | Sink responsibility using `EventId` or stream coordinates |

The system should not use “exactly once” terminology. Local crash recovery and sink idempotency can reduce duplicates, but the architecture should remain honest.

## Retention Model

Retention is topic-level policy.

```yaml
topics:
  agent.output:
    partitions: 4
    retention:
      maxBytes: 20GB
      maxAge: 7d
      deletePolicy: delete
```

Retention is independent of subscription progress. A dead or paused subscription may fall behind retention. If its committed offset points to deleted data, the subscription enters an explicit state:

```text
OffsetExpired
```

The operator must then reset it to:

```text
earliest
latest
specific offset
specific timestamp
```

For Agent output, LocalStream retention should be enough for local restart and backpressure tolerance. The Data Lake archive remains the long-term forensic store.

## Agent Pipeline Mapping

DeltaZulu Agent output should use LocalStream like this:

```text
Agent output
  -> LocalStream topic: agent.output

archive subscription
  -> reads agent.output
  -> writes Data Lake / archive
  -> commits offsets

silver subscription
  -> reads agent.output
  -> normalizes field names / NRT Silver
  -> commits offsets

logcluster subscription
  -> reads agent.output
  -> filters no_parser / parse_failed
  -> samples / deduplicates
  -> writes logcluster.samples
  -> commits offsets
```

The LogCluster path is not evidence storage. It may sample and drop under pressure. The archive path is the evidence path.

## Medallion Alignment

The NRT pipeline should reflect the medallion model:

```text
agent.output
  -> NRT Bronze raw stream
  -> Silver normalized streams/tables
  -> Golden detection/query streams
```

The Data Lake archive also receives the raw stream for long-term storage and forensic replay.

LocalStream is the local substrate that allows these branches to read independently without forcing the Agent to write multiple physical copies.

## DurableBuffer Relationship

DurableBuffer should remain a simple queue/buffer library. It has a catalog-backed queue lifecycle: records are written, sealed, added to a catalog, dispatched into a bounded channel, then completed, released, or dead-lettered by the consumer.

LocalStream should not reuse DurableBuffer’s delete-on-complete model for topic storage, because stream retention is not the same as queue completion.

However, DurableBuffer remains useful for:

```text
edge ingress buffering
external sink retry buffers
dead-letter overflow buffers
temporary spooling
processor output buffering
```

The recent DurableBuffer additions help as a local queue substrate. It now has configurable dispatch channel capacity and max in-flight chunk settings.  It also exposes dispatch-related snapshot fields such as queue depth, in-flight count, available chunks, oldest available age, oldest dispatched age, and wait reason.

Those are useful for service-local queue health, but LocalStream still needs its own stream-log metrics.

## Backpressure

Backpressure is primarily handled by DurableBuffer, but it must be explicitly configured and tuned by the using library to match the expected workload and delivery guarantees.

Backpressure should exist at several levels:

```text
topic append pressure
partition segment pressure
subscription lag pressure
processor lag pressure
external sink pressure
disk retention pressure
```

Suggested behavior:

| Component             | Backpressure behavior                               |
| --------------------- | --------------------------------------------------- |
| `agent.output` append | Block or reject based on Agent policy               |
| archive consumer      | Required; alert and retry                           |
| silver consumer       | Usually required for NRT completeness; configurable |
| logcluster consumer   | Optional; sample/drop under pressure                |
| debug-local consumer  | Optional; drop or reject                            |

Optional consumers must not block archive durability.

## Processor Model

Processors should be local and explicit.

Example processors:

```text
parser-status-classifier
silver-normalizer
logcluster-sampler
```

Processor flow:

```text
input topic
  -> processor
  -> output topic or external sink
  -> commit input offset after successful output
```

A processor should commit offsets only after its side effect is complete. For output-to-topic processors, this means append output first, then commit input. Duplicates are possible after crash, so output records need deterministic IDs where possible.

## Configuration Example

```yaml
localStream:
  storagePath: ./data/localstream

  topics:
    - name: agent.output
      partitions: 4
      retention:
        maxBytes: 21474836480
        maxAge: 7d

    - name: logcluster.samples
      partitions: 1
      retention:
        maxBytes: 1073741824
        maxAge: 3d

  subscriptions:
    - id: archive
      topic: agent.output
      required: true
      startPosition: earliest

    - id: silver
      topic: agent.output
      required: true
      startPosition: earliest

    - id: logcluster
      topic: agent.output
      required: false
      startPosition: latest
      filter: parserStatus in ["no_parser", "parse_failed"]
      sampling:
        mode: bounded-deduplicated
        maxPerSourcePerMinute: 1000
```

## Metrics

LocalStream should expose topic, partition, subscription, and processor metrics.

Topic metrics:

```text
localstream_topic_bytes
localstream_topic_records_total
localstream_topic_segments
localstream_topic_oldest_record_age_seconds
localstream_topic_newest_offset
```

Partition metrics:

```text
localstream_partition_append_latency_ms
localstream_partition_offset
localstream_partition_segment_count
localstream_partition_retention_deletes_total
```

Subscription metrics:

```text
localstream_subscription_committed_offset
localstream_subscription_lag_records
localstream_subscription_lag_bytes
localstream_subscription_oldest_uncommitted_age_seconds
localstream_subscription_reads_total
localstream_subscription_commits_total
localstream_subscription_offset_expired_total
```

Processor metrics:

```text
localstream_processor_records_in_total
localstream_processor_records_out_total
localstream_processor_failures_total
localstream_processor_checkpoint_lag_seconds
localstream_processor_state_bytes
```

## Failure Handling

### Crash During Append

An append is successful only after the record and index state are durable according to the selected flush policy. If a segment has a partial tail after crash, recovery truncates to the last valid indexed/checksummed record or quarantines the segment.

### Crash Before Commit

The consumer may receive the record again after restart. This is expected at-least-once behavior.

### Subscription Offset Expired

If retention deletes records before a subscription reads them, the subscription enters `OffsetExpired`. It must be manually or automatically reset according to policy.

### Processor Failure

Processor failure does not delete input records. The processor restarts from its last committed checkpoint. Output sinks must be idempotent.

## Security and Safety

LocalStream should apply the same local-file safety mindset as DurableBuffer:

```text
owner-only file permissions on Unix
symlink protection
atomic file writes and renames
bounded quarantine
bounded dead-letter storage
max record size
max segment size
checksum validation
metadata versioning
safe startup recovery
```

DurableBuffer already documents symlink protection, Unix file mode hardening, and atomic writes.  LocalStream should follow the same baseline.

## Test Strategy

Minimum test suite:

```text
append assigns monotonic offsets
read from earliest returns all retained records
read from committed resumes correctly
commit survives restart
consumer sees duplicate after crash before commit
partition ordering is preserved
two subscriptions read the same records independently
retention deletes old segments by policy
expired subscription enters OffsetExpired
processor writes output before committing input
logcluster subscription can drop/sample without affecting archive
```

Integration tests for Agent use:

```text
archive and silver both read agent.output
logcluster reads only parser-failed samples
archive lag does not change silver offsets
silver failure does not corrupt archive offsets
restart preserves all committed offsets
```

## Roadmap

### Phase 1: Local Stream Log

Implement:

```text
topics
partitions
append-only segments
offset indexes
producer append
consumer read
subscription checkpoints
retention
basic metrics
```

### Phase 2: Agent Output Integration

Implement:

```text
agent.output topic
archive subscription
silver subscription
logcluster subscription
configuration binding
operational metrics
```

### Phase 3: Local Processors

Implement:

```text
processor API
parser-status processor
silver-normalizer processor
logcluster-sampler processor
processor checkpoints
```

### Phase 4: Advanced Recovery and Operations

Implement:

```text
segment repair tooling
subscription reset tooling
manual replay tooling
lag dashboards
retention audit
```

### Phase 5: Optional Optimizations

Only if needed:

```text
compression
batch append
zero-copy reads
memory-mapped indexes
topic compaction
state stores
windowed processing
```

## Final Decision

The correct name for the new library is:

```text
DeltaZulu.LocalStream
```

The correct architecture is:

```text
local, durable, append-only stream log
with topics, partitions, offsets, subscriptions, replay, retention, and processors
```

The correct boundary is:

```text
DurableBuffer remains a simple local queue/buffer primitive.
LocalStream provides Kafka/Pulsar/Flink-inspired local stream semantics.
```

Do not add durable broadcast subscribers, topics, offsets, or stream processors to DurableBuffer itself. That would turn the buffer into an embedded broker and damage its current simplicity.
