namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class MetricsTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task TopicMetrics_ReportOffsetsBytesAndSegments()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(
            TestHost.Options(dir, partitions: 2, maxSegmentBytes: 256));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 10; i++)
        {
            await producer.AppendAsync(
                "agent.output",
                new TestEvent("s", $"m{i}"),
                new AppendOptions { Partition = i % 2 });
        }

        var metrics = host.GetTopicMetrics("agent.output");

        Assert.AreEqual("agent.output", metrics.Topic);
        Assert.AreEqual(10, metrics.RecordsTotal);
        Assert.IsTrue(metrics.SizeBytes > 0);
        Assert.AreEqual(2, metrics.Partitions.Count);
        Assert.AreEqual(5, metrics.Partitions[0].NextOffset);
        Assert.AreEqual(5, metrics.Partitions[1].NextOffset);
        Assert.AreEqual(0, metrics.Partitions[0].EarliestRetainedOffset);
        Assert.IsTrue(metrics.SegmentCount >= 2, "small segments must have rolled");
        Assert.AreEqual(metrics.SegmentCount, metrics.Partitions.Sum(p => p.SegmentCount));
        Assert.AreEqual(metrics.SizeBytes, metrics.Partitions.Sum(p => p.SizeBytes));
    }

    [TestMethod]
    public async Task SubscriptionMetrics_ReportLagAndCommittedOffset()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 10; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[2].Position);

        var metrics = host.GetSubscriptionMetrics("archive", "agent.output");

        Assert.AreEqual("archive", metrics.SubscriptionId);
        Assert.AreEqual("agent.output", metrics.Topic);
        Assert.AreEqual(7, metrics.TotalLagRecords, "10 appended, 3 consumed");
        Assert.AreEqual(1, metrics.Partitions.Count);
        Assert.AreEqual(2, metrics.Partitions[0].CommittedOffset);
        Assert.AreEqual(SubscriptionState.Active, metrics.Partitions[0].State);
    }

    [TestMethod]
    public async Task SubscriptionMetrics_WithoutCheckpoint_LagCountsAllRetainedRecords()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 4; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var metrics = host.GetSubscriptionMetrics("never-read", "agent.output");

        Assert.AreEqual(4, metrics.TotalLagRecords);
        Assert.AreEqual(-1, metrics.Partitions[0].CommittedOffset, "-1 means no checkpoint");
    }

    [TestMethod]
    public async Task SubscriptionMetrics_SurfaceRequiredFlag_AndOffsetExpiredState()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = TestHost.Options(dir, retentionMaxBytes: 1024, maxSegmentBytes: 512);
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "archive",
            Topic = "agent.output",
            Required = true,
        });
        await using var host = await TestHost.StartAsync(options);

        var producer = host.CreateProducer<TestEvent>();
        var first = await producer.AppendAsync("agent.output", new TestEvent("s", "first"));
        var consumer = host.CreateConsumer<TestEvent>("archive");
        await consumer.CommitAsync(first.Position!);

        for (var i = 0; i < 40; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"padded-message-{i:D6}"));
        }

        await host.ApplyRetentionAsync();

        var metrics = host.GetSubscriptionMetrics("archive", "agent.output");

        Assert.IsTrue(metrics.Required);
        Assert.AreEqual(SubscriptionState.OffsetExpired, metrics.Partitions[0].State);
    }
}
