namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class AppendReadTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task Append_AssignsMonotonicOffsets_PerPartition()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir, partitions: 2));
        var producer = host.CreateProducer<TestEvent>();

        for (var expected = 0L; expected < 5; expected++)
        {
            var p0 = await producer.AppendAsync(
                "agent.output",
                new TestEvent("host-a", $"p0-{expected}"),
                new AppendOptions { Partition = 0 });
            var p1 = await producer.AppendAsync(
                "agent.output",
                new TestEvent("host-b", $"p1-{expected}"),
                new AppendOptions { Partition = 1 });

            Assert.AreEqual(AppendStatus.Appended, p0.Status);
            Assert.AreEqual(AppendStatus.Appended, p1.Status);
            Assert.AreEqual(new StreamPosition("agent.output", 0, expected), p0.Position);
            Assert.AreEqual(new StreamPosition("agent.output", 1, expected), p1.Position);
        }
    }

    [TestMethod]
    public async Task Append_AssignsStableEventId_AndReturnsIt()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        var generated = await producer.AppendAsync("agent.output", new TestEvent("s", "auto-id"));
        var explicitId = await producer.AppendAsync(
            "agent.output",
            new TestEvent("s", "explicit-id"),
            new AppendOptions { EventId = "evt-42" });

        Assert.IsFalse(string.IsNullOrWhiteSpace(generated.EventId));
        Assert.AreEqual("evt-42", explicitId.EventId);

        var consumer = host.CreateConsumer<TestEvent>("debug-local");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

        Assert.AreEqual(generated.EventId, records[0].EventId);
        Assert.AreEqual("evt-42", records[1].EventId);
    }

    [TestMethod]
    public async Task Append_ToUnknownTopic_IsRejected()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        var result = await producer.AppendAsync("no.such.topic", new TestEvent("s", "m"));

        Assert.AreEqual(AppendStatus.RejectedTopicNotFound, result.Status);
        Assert.IsNull(result.Position);
    }

    [TestMethod]
    public async Task Read_FromEarliest_ReturnsAllRetainedRecords()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 10; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

        Assert.AreEqual(10, records.Count);
        for (var i = 0; i < 10; i++)
        {
            Assert.AreEqual(i, records[i].Offset);
            Assert.AreEqual($"m{i}", records[i].Payload.Message);
            Assert.AreEqual("agent.output", records[i].Topic);
        }
    }

    [TestMethod]
    public async Task PartitionOrdering_IsPreserved()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir, partitions: 3));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 50; i++)
        {
            // Same partition key must land in one partition, preserving order.
            await producer.AppendAsync(
                "agent.output",
                new TestEvent("source-x", $"seq-{i:D4}"),
                new AppendOptions { PartitionKey = "source-x" });
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

        Assert.AreEqual(50, records.Count);
        var partition = records[0].Partition;
        for (var i = 0; i < 50; i++)
        {
            Assert.AreEqual(partition, records[i].Partition);
            Assert.AreEqual($"seq-{i:D4}", records[i].Payload.Message);
            if (i > 0)
            {
                Assert.AreEqual(records[i - 1].Offset + 1, records[i].Offset);
            }
        }
    }

    [TestMethod]
    public async Task Restart_RecoversAppendedRecordsFromDisk()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var eventIds = new List<string?>();

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 7; i++)
            {
                var result = await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
                eventIds.Add(result.EventId);
            }
        }

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(
                consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

            Assert.AreEqual(7, records.Count);
            CollectionAssert.AreEqual(eventIds, records.Select(r => r.EventId).ToList());

            // New appends continue the offset sequence after recovery.
            var producer = host.CreateProducer<TestEvent>();
            var appended = await producer.AppendAsync("agent.output", new TestEvent("s", "after-restart"));
            Assert.AreEqual(7, appended.Position!.Offset);
        }
    }

    [TestMethod]
    public async Task Restart_TruncatesPartialTail_AndKeepsValidRecords()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 3; i++)
            {
                await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
            }
        }

        // Simulate a crash mid-append: garbage partial tail on the active segment.
        var segment = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Single();
        await File.AppendAllTextAsync(segment, "deadbeef {\"truncated");

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(
                consumer, "agent.output", new ReadOptions { Start = ReadStart.Earliest });

            Assert.AreEqual(3, records.Count);

            var producer = host.CreateProducer<TestEvent>();
            var appended = await producer.AppendAsync("agent.output", new TestEvent("s", "after-repair"));
            Assert.AreEqual(AppendStatus.Appended, appended.Status);
            Assert.AreEqual(3, appended.Position!.Offset);
        }
    }
}
