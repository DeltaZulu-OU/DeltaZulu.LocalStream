namespace DeltaZulu.LocalStream.Tests;

/// <summary>
/// Manual replay tooling: one-off reads from an explicit offset or timestamp
/// that never touch the subscription's committed checkpoint.
/// </summary>
[TestClass]
public sealed class ReplayTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task Read_FromExplicitOffset_ReplaysFromThatOffset()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 6; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("debug-local");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", ReadOptions.FromOffset(4));

        Assert.AreEqual(2, records.Count);
        Assert.AreEqual(4, records[0].Offset);
        Assert.AreEqual(5, records[1].Offset);
    }

    [TestMethod]
    public async Task Read_FromTimestamp_ReplaysRecordsPublishedAtOrAfterIt()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        await producer.AppendAsync("agent.output", new TestEvent("s", "before"));
        await Task.Delay(30);
        var cutoff = DateTimeOffset.UtcNow;
        await Task.Delay(30);
        await producer.AppendAsync("agent.output", new TestEvent("s", "after-1"));
        await producer.AppendAsync("agent.output", new TestEvent("s", "after-2"));

        var consumer = host.CreateConsumer<TestEvent>("debug-local");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", ReadOptions.FromTimestamp(cutoff));

        Assert.AreEqual(2, records.Count);
        Assert.AreEqual("after-1", records[0].Payload.Message);
    }

    [TestMethod]
    public async Task Read_FromTimestampPastEnd_ReturnsNothing()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();
        await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));

        var consumer = host.CreateConsumer<TestEvent>("debug-local");
        var records = await TestHost.ReadAllAsync(
            consumer, "agent.output", ReadOptions.FromTimestamp(DateTimeOffset.UtcNow.AddHours(1)));

        Assert.AreEqual(0, records.Count);
    }

    [TestMethod]
    public async Task ReplayRead_DoesNotDisturbCommittedCheckpoint()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        for (var i = 0; i < 5; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[3].Position);

        // Operator replays history through the same subscription id.
        var replayed = await TestHost.ReadAllAsync(
            consumer, "agent.output", ReadOptions.FromOffset(0));
        Assert.AreEqual(5, replayed.Count);

        // The committed position is untouched: a normal read resumes at 4.
        var resumed = await TestHost.ReadAllAsync(consumer, "agent.output");
        Assert.AreEqual(1, resumed.Count);
        Assert.AreEqual(4, resumed[0].Offset);
    }
}
