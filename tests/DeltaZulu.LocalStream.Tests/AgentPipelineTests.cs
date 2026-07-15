namespace DeltaZulu.LocalStream.Tests;

/// <summary>
/// Integration tests for the DeltaZulu.Platform agent output topology from the
/// architecture doc: archive, silver, and logcluster subscriptions reading
/// agent.output independently.
/// </summary>
[TestClass]
public sealed class AgentPipelineTests
{
    public TestContext TestContext { get; set; } = null!;

    private sealed record AgentEvent(string Source, string ParserStatus, string Raw);

    private static LocalStreamOptions PipelineOptions(string dir)
    {
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions { Name = "agent.output", Partitions = 2 });
        options.Topics.Add(new TopicOptions { Name = "logcluster.samples", Partitions = 1 });
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "archive",
            Topic = "agent.output",
            Required = true,
        });
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "silver",
            Topic = "agent.output",
            Required = true,
        });
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "logcluster",
            Topic = "agent.output",
            StartPosition = StartPosition.Latest,
        });
        return options;
    }

    private static async Task AppendAgentEventsAsync(LocalStreamHost host, int count)
    {
        var producer = host.CreateProducer<AgentEvent>();
        for (var i = 0; i < count; i++)
        {
            var status = i % 3 == 0 ? "parse_failed" : "ok";
            await producer.AppendAsync(
                "agent.output",
                new AgentEvent($"host-{i % 4}", status, $"raw-{i}"),
                new AppendOptions { PartitionKey = $"host-{i % 4}" });
        }
    }

    private static async Task<List<StreamRecord<AgentEvent>>> ReadAllAsync(
        ILocalStreamConsumer<AgentEvent> consumer, string topic)
    {
        var records = new List<StreamRecord<AgentEvent>>();
        await foreach (var record in consumer.ReadAsync(topic))
        {
            records.Add(record);
        }

        return records;
    }

    [TestMethod]
    public async Task ArchiveAndSilver_BothReadAgentOutput()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(PipelineOptions(dir));
        await AppendAgentEventsAsync(host, 12);

        var archive = host.CreateConsumer<AgentEvent>("archive");
        var silver = host.CreateConsumer<AgentEvent>("silver");

        var archived = await ReadAllAsync(archive, "agent.output");
        var normalized = await ReadAllAsync(silver, "agent.output");

        Assert.AreEqual(12, archived.Count);
        Assert.AreEqual(12, normalized.Count);
        CollectionAssert.AreEquivalent(
            archived.Select(r => r.EventId).ToList(),
            normalized.Select(r => r.EventId).ToList());
    }

    [TestMethod]
    public async Task ArchiveLag_DoesNotChangeSilverOffsets()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(PipelineOptions(dir));
        await AppendAgentEventsAsync(host, 12);

        // Silver consumes and commits everything; archive never reads.
        var silver = host.CreateConsumer<AgentEvent>("silver");
        foreach (var record in await ReadAllAsync(silver, "agent.output"))
        {
            await silver.CommitAsync(record.Position);
        }

        Assert.AreEqual(0, host.GetSubscriptionMetrics("silver", "agent.output").TotalLagRecords);
        Assert.AreEqual(12, host.GetSubscriptionMetrics("archive", "agent.output").TotalLagRecords);

        // Archive catching up later must not disturb silver's committed offsets.
        var archive = host.CreateConsumer<AgentEvent>("archive");
        foreach (var record in await ReadAllAsync(archive, "agent.output"))
        {
            await archive.CommitAsync(record.Position);
        }

        Assert.AreEqual(0, host.GetSubscriptionMetrics("silver", "agent.output").TotalLagRecords);
        Assert.AreEqual(0, (await ReadAllAsync(silver, "agent.output")).Count);
    }

    [TestMethod]
    public async Task SilverFailure_DoesNotCorruptArchiveOffsets()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(PipelineOptions(dir));
        await AppendAgentEventsAsync(host, 12);

        var archive = host.CreateConsumer<AgentEvent>("archive");
        foreach (var record in await ReadAllAsync(archive, "agent.output"))
        {
            await archive.CommitAsync(record.Position);
        }

        // Silver reads but crashes before committing anything.
        var silver = host.CreateConsumer<AgentEvent>("silver");
        _ = await ReadAllAsync(silver, "agent.output");

        Assert.AreEqual(0, host.GetSubscriptionMetrics("archive", "agent.output").TotalLagRecords);
        Assert.AreEqual(12, host.GetSubscriptionMetrics("silver", "agent.output").TotalLagRecords);

        // Silver redelivers everything; archive stays fully committed.
        Assert.AreEqual(12, (await ReadAllAsync(silver, "agent.output")).Count);
        Assert.AreEqual(0, (await ReadAllAsync(archive, "agent.output")).Count);
    }

    [TestMethod]
    public async Task Logcluster_ReadsOnlyParserFailedSamples()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(PipelineOptions(dir));
        await AppendAgentEventsAsync(host, 12);

        // logcluster filters parser failures and forwards them to its sample
        // topic; dropping records must not affect the archive path.
        var logcluster = host.CreateConsumer<AgentEvent>("logcluster");
        var sampleProducer = host.CreateProducer<AgentEvent>();
        await foreach (var record in logcluster.ReadAsync(
            "agent.output", new ReadOptions { Start = ReadStart.Earliest }))
        {
            if (record.Payload.ParserStatus is "no_parser" or "parse_failed")
            {
                await sampleProducer.AppendAsync(
                    "logcluster.samples",
                    record.Payload,
                    new AppendOptions { EventId = record.EventId });
            }

            await logcluster.CommitAsync(record.Position);
        }

        var samples = host.CreateConsumer<AgentEvent>("debug-local");
        var sampled = await ReadAllAsync(samples, "logcluster.samples");

        Assert.AreEqual(4, sampled.Count, "every third of 12 events is parse_failed");
        Assert.IsTrue(sampled.All(r => r.Payload.ParserStatus == "parse_failed"));

        var archive = host.CreateConsumer<AgentEvent>("archive");
        Assert.AreEqual(12, (await ReadAllAsync(archive, "agent.output")).Count);
    }

    [TestMethod]
    public async Task Restart_PreservesAllCommittedOffsets()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(PipelineOptions(dir)))
        {
            await AppendAgentEventsAsync(host, 12);

            var archive = host.CreateConsumer<AgentEvent>("archive");
            foreach (var record in await ReadAllAsync(archive, "agent.output"))
            {
                await archive.CommitAsync(record.Position);
            }

            var silver = host.CreateConsumer<AgentEvent>("silver");
            var silverRecords = await ReadAllAsync(silver, "agent.output");
            foreach (var record in silverRecords.Take(5))
            {
                await silver.CommitAsync(record.Position);
            }
        }

        await using (var host = await TestHost.StartAsync(PipelineOptions(dir)))
        {
            Assert.AreEqual(0, host.GetSubscriptionMetrics("archive", "agent.output").TotalLagRecords);

            var archive = host.CreateConsumer<AgentEvent>("archive");
            Assert.AreEqual(0, (await ReadAllAsync(archive, "agent.output")).Count);

            var silver = host.CreateConsumer<AgentEvent>("silver");
            var remaining = await ReadAllAsync(silver, "agent.output");
            Assert.IsTrue(remaining.Count > 0 && remaining.Count < 12);
            Assert.AreEqual(
                12 - remaining.Count,
                host.GetSubscriptionMetrics("silver", "agent.output").Partitions.Sum(p => p.CommittedOffset + 1));
        }
    }
}
