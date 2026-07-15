namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class RetentionTests
{
    public TestContext TestContext { get; set; } = null!;

    private const int RecordCount = 40;

    private static LocalStreamOptions RetentionOptions(string dir) => TestHost.Options(
        dir,
        retentionMaxBytes: 1024,
        maxSegmentBytes: 512);

    private static async Task AppendManyAsync(LocalStreamHost host, int count)
    {
        var producer = host.CreateProducer<TestEvent>();
        for (var i = 0; i < count; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"padded-message-{i:D6}"));
        }
    }

    [TestMethod]
    public async Task Retention_DeletesOldSegmentsByPolicy()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(RetentionOptions(dir));
        await AppendManyAsync(host, RecordCount);

        var segmentsBefore = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();
        Assert.IsTrue(segmentsBefore > 2, "test setup must produce multiple segments");

        await host.ApplyRetentionAsync();

        var segmentsAfter = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();
        Assert.IsTrue(segmentsAfter < segmentsBefore, "retention must delete old segments");

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var retained = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

        Assert.IsTrue(retained.Count > 0, "the active segment is never deleted");
        Assert.IsTrue(retained.Count < RecordCount, "old records must be gone");
        Assert.IsTrue(retained[0].Offset > 0, "earliest retained offset moved forward");
        Assert.AreEqual(RecordCount - 1, retained[^1].Offset, "newest records are retained");
    }

    [TestMethod]
    public async Task Retention_IsIndependentOfSubscriptionProgress()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(RetentionOptions(dir));
        await AppendManyAsync(host, RecordCount);

        // A subscription that never commits must not prevent deletion.
        _ = host.CreateConsumer<TestEvent>("lagging");

        var segmentsBefore = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();

        await host.ApplyRetentionAsync();

        var segmentsAfter = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();
        Assert.IsTrue(segmentsAfter < segmentsBefore);
    }

    [TestMethod]
    public async Task ExpiredSubscription_EntersOffsetExpired_AndRecoversByReset()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(RetentionOptions(dir));

        var producer = host.CreateProducer<TestEvent>();
        var first = await producer.AppendAsync("agent.output", new TestEvent("s", "first"));

        var consumer = host.CreateConsumer<TestEvent>("logcluster");
        await consumer.CommitAsync(first.Position!);

        // Retention now deletes far past the committed offset.
        await AppendManyAsync(host, RecordCount);
        await host.ApplyRetentionAsync();

        Assert.AreEqual(
            SubscriptionState.OffsetExpired,
            host.GetSubscriptionState("logcluster", "agent.output", partition: 0));

        await Assert.ThrowsExactlyAsync<OffsetExpiredException>(
            async () => await TestHost.ReadAllAsync(consumer, "agent.output"));

        // Operator resets to earliest; the subscription becomes readable again.
        await consumer.ResetAsync("agent.output", ResetPosition.Earliest());

        Assert.AreEqual(
            SubscriptionState.Active,
            host.GetSubscriptionState("logcluster", "agent.output", partition: 0));

        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.IsTrue(records.Count > 0);
        Assert.IsTrue(records[0].Offset > first.Position!.Offset);
    }

    [TestMethod]
    public async Task FreshSubscription_AfterRetention_IsNotExpired()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(RetentionOptions(dir));
        await AppendManyAsync(host, RecordCount);
        await host.ApplyRetentionAsync();

        // No checkpoint yet: starts at earliest retained, not OffsetExpired.
        var consumer = host.CreateConsumer<TestEvent>("late-joiner");

        Assert.AreEqual(
            SubscriptionState.Active,
            host.GetSubscriptionState("late-joiner", "agent.output", partition: 0));

        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.IsTrue(records.Count > 0);
        Assert.IsTrue(records[0].Offset > 0);
    }
}
