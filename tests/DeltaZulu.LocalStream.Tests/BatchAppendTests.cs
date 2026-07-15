namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class BatchAppendTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task AppendBatch_AssignsContiguousOffsets_InInputOrder()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        var batch = Enumerable.Range(0, 25)
            .Select(i => new TestEvent("s", $"b{i}"))
            .ToList();

        var results = await producer.AppendBatchAsync("agent.output", batch);

        Assert.AreEqual(25, results.Count);
        for (var i = 0; i < 25; i++)
        {
            Assert.AreEqual(AppendStatus.Appended, results[i].Status);
            Assert.AreEqual(i, results[i].Position!.Offset);
        }
    }

    [TestMethod]
    public async Task AppendBatch_WithPartitionKey_KeepsBatchInOnePartitionInOrder()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir, partitions: 3));
        var producer = host.CreateProducer<TestEvent>();

        var batch = Enumerable.Range(0, 10)
            .Select(i => new TestEvent("s", $"seq-{i}"))
            .ToList();

        var results = await producer.AppendBatchAsync(
            "agent.output", batch, new AppendOptions { PartitionKey = "source-x" });

        var partition = results[0].Position!.Partition;
        for (var i = 0; i < 10; i++)
        {
            Assert.AreEqual(partition, results[i].Position!.Partition);
            Assert.AreEqual(i, results[i].Position!.Offset);
        }
    }

    [TestMethod]
    public async Task AppendBatch_IsDurable_AcrossRestart()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendBatchAsync(
                "agent.output",
                Enumerable.Range(0, 8).Select(i => new TestEvent("s", $"b{i}")).ToList());
        }

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(
                consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

            Assert.AreEqual(8, records.Count);
            for (var i = 0; i < 8; i++)
            {
                Assert.AreEqual($"b{i}", records[i].Payload.Message);
            }
        }
    }

    [TestMethod]
    public async Task AppendBatch_RollsSegments_WhenBatchExceedsSegmentSize()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(
            TestHost.Options(dir, maxSegmentBytes: 512));
        var producer = host.CreateProducer<TestEvent>();

        await producer.AppendBatchAsync(
            "agent.output",
            Enumerable.Range(0, 30).Select(i => new TestEvent("s", $"padded-message-{i:D6}")).ToList());

        Assert.IsTrue(host.GetTopicMetrics("agent.output").SegmentCount > 1);

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });
        Assert.AreEqual(30, records.Count);
    }

    [TestMethod]
    public async Task AppendBatch_UnknownTopic_RejectsWholeBatch()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        var results = await producer.AppendBatchAsync(
            "no.such.topic",
            [new TestEvent("s", "m0"), new TestEvent("s", "m1")]);

        Assert.AreEqual(2, results.Count);
        Assert.IsTrue(results.All(r => r.Status == AppendStatus.RejectedTopicNotFound));
    }
}
