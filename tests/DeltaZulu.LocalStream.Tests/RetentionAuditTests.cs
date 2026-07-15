namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class RetentionAuditTests
{
    public TestContext TestContext { get; set; } = null!;

    private static LocalStreamOptions SmallSegments(string dir) => TestHost.Options(
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
    public async Task AuditRetention_ReportsViolatingSegments_WithoutDeleting()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(SmallSegments(dir));
        await AppendManyAsync(host, 40);

        var segmentsBefore = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();

        var audit = host.AuditRetention("agent.output");

        Assert.AreEqual("agent.output", audit.Topic);
        Assert.IsTrue(audit.DeletableSegments > 0, "size cap is exceeded, deletions must be pending");
        Assert.IsTrue(audit.DeletableBytes > 0);
        Assert.IsTrue(audit.DeletableRecords > 0);

        // Audit is a dry run: nothing was deleted.
        var segmentsAfter = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Count();
        Assert.AreEqual(segmentsBefore, segmentsAfter);

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });
        Assert.AreEqual(40, records.Count);
    }

    [TestMethod]
    public async Task AuditRetention_MatchesWhatApplyRetentionDeletes()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(SmallSegments(dir));
        await AppendManyAsync(host, 40);

        var audit = host.AuditRetention("agent.output");
        var predictedFirstRetained = audit.Partitions[0].FirstRetainedOffsetAfterDeletion;

        await host.ApplyRetentionAsync();

        var metrics = host.GetTopicMetrics("agent.output");
        Assert.AreEqual(predictedFirstRetained, metrics.Partitions[0].EarliestRetainedOffset);

        // Everything violating was deleted; a fresh audit is clean.
        var after = host.AuditRetention("agent.output");
        Assert.AreEqual(0, after.DeletableSegments);
        Assert.AreEqual(0, after.DeletableBytes);
    }

    [TestMethod]
    public async Task AuditRetention_NoPolicyViolation_ReportsClean()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        await AppendManyAsync(host, 5);

        var audit = host.AuditRetention("agent.output");

        Assert.AreEqual(0, audit.DeletableSegments);
        Assert.AreEqual(0, audit.DeletableRecords);
        Assert.AreEqual(0, audit.Partitions[0].DeletableSegments);
    }
}
