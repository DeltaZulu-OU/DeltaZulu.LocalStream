# DeltaZulu.LocalStream

A local, durable, append-only stream log for .NET — Kafka/Pulsar/Flink-inspired, not Kafka-compatible. Topics, partitions, monotonic offsets, named subscriptions with durable checkpoints, replay, and policy-based retention, all without an external broker.

See [docs/LOCAL_STREAM_ARCHITECTURE.md](docs/LOCAL_STREAM_ARCHITECTURE.md) for the full architecture, and [DeltaZulu.DurableBuffer](https://github.com/DeltaZulu-OU/DeltaZulu.DurableBuffer) (included as a git submodule under `external/`) for the sibling durable queue primitive used at service edges.

## Getting started

Clone with submodules:

```bash
git clone --recurse-submodules https://github.com/DeltaZulu-OU/DeltaZulu.LocalStream.git
# or, in an existing clone:
git submodule update --init --recursive
```

Build and test (requires the .NET 10 SDK):

```bash
dotnet build DeltaZulu.LocalStream.slnx
dotnet test DeltaZulu.LocalStream.slnx
```

## Usage

```csharp
var host = new LocalStreamHost(new LocalStreamOptions
{
    StoragePath = "./data/localstream",
    Topics =
    {
        new TopicOptions
        {
            Name = "agent.output",
            Partitions = 4,
            Retention = new RetentionOptions { MaxBytes = 20L * 1024 * 1024 * 1024, MaxAge = TimeSpan.FromDays(7) },
        },
    },
});
await host.StartAsync();

// Append once...
var producer = host.CreateProducer<AgentEvent>();
await producer.AppendAsync("agent.output", theEvent, new AppendOptions { PartitionKey = theEvent.Source });

// ...read many times, each subscription with its own durable offset.
var archive = host.CreateConsumer<AgentEvent>("archive");
await foreach (var record in archive.ReadAsync("agent.output"))
{
    await WriteToDataLakeAsync(record);
    await archive.CommitAsync(record.Position);
}
```

Delivery is at-least-once; ordering is preserved within a partition; retention is independent of subscription progress (a lagging subscription enters `OffsetExpired` and must be reset). Sinks should use `StreamRecord.EventId` or stream coordinates for idempotency.
