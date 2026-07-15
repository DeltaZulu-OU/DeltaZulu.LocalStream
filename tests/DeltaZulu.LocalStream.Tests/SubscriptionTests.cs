namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class SubscriptionTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task Read_FromCommitted_ResumesAfterCommittedOffset()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 5; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var first = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.AreEqual(5, first.Count, "fresh subscription starts from earliest");

        await consumer.CommitAsync(first[2].Position);

        var resumed = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.AreEqual(2, resumed.Count);
        Assert.AreEqual(3, resumed[0].Offset);
        Assert.AreEqual(4, resumed[1].Offset);
    }

    [TestMethod]
    public async Task Commit_SurvivesRestart()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 5; i++)
            {
                await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
            }

            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            await consumer.CommitAsync(records[2].Position);
        }

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var resumed = await TestHost.ReadAllAsync(consumer, "agent.output");

            Assert.AreEqual(2, resumed.Count);
            Assert.AreEqual(3, resumed[0].Offset);
        }
    }

    [TestMethod]
    public async Task CrashBeforeCommit_ConsumerSeesDuplicate()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        string? readButNotCommitted;

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));
            await producer.AppendAsync("agent.output", new TestEvent("s", "m1"));

            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            await consumer.CommitAsync(records[0].Position);

            // Record at offset 1 was read but its commit never happened (crash).
            readButNotCommitted = records[1].EventId;
        }

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var redelivered = await TestHost.ReadAllAsync(consumer, "agent.output");

            Assert.AreEqual(1, redelivered.Count, "at-least-once: uncommitted record is redelivered");
            Assert.AreEqual(readButNotCommitted, redelivered[0].EventId);
            Assert.AreEqual(1, redelivered[0].Offset);
        }
    }

    [TestMethod]
    public async Task TwoSubscriptions_ReadSameRecordsIndependently()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 6; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var archive = host.CreateConsumer<TestEvent>("archive");
        var silver = host.CreateConsumer<TestEvent>("silver");

        var archiveRecords = await TestHost.ReadAllAsync(archive, "agent.output");
        await archive.CommitAsync(archiveRecords[^1].Position);

        // Archive committing everything must not advance silver.
        var silverRecords = await TestHost.ReadAllAsync(silver, "agent.output");

        Assert.AreEqual(6, archiveRecords.Count);
        Assert.AreEqual(6, silverRecords.Count);
        CollectionAssert.AreEqual(
            archiveRecords.Select(r => r.EventId).ToList(),
            silverRecords.Select(r => r.EventId).ToList());

        // Silver commits partial progress; archive stays fully committed.
        await silver.CommitAsync(silverRecords[1].Position);

        Assert.AreEqual(0, (await TestHost.ReadAllAsync(archive, "agent.output")).Count);
        Assert.AreEqual(4, (await TestHost.ReadAllAsync(silver, "agent.output")).Count);
    }

    [TestMethod]
    public async Task Reset_ToEarliest_ReplaysAllRetainedRecords()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 4; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[^1].Position);
        Assert.AreEqual(0, (await TestHost.ReadAllAsync(consumer, "agent.output")).Count);

        await consumer.ResetAsync("agent.output", ResetPosition.Earliest());

        var replayed = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.AreEqual(4, replayed.Count);
        Assert.AreEqual(0, replayed[0].Offset);
    }

    [TestMethod]
    public async Task Reset_ToOffset_ReplaysFromThatOffset()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 4; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[^1].Position);

        await consumer.ResetAsync("agent.output", ResetPosition.AtOffset(2));

        var replayed = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.AreEqual(2, replayed.Count);
        Assert.AreEqual(2, replayed[0].Offset);
    }
}
