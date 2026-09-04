namespace DeltaZulu.LocalStream.Tests;

/// <summary>
/// Phase 0 correctness fixes: SafeFiles fsync (verified by review — a
/// power-loss harness is out of scope for a unit test, so these tests cover
/// the corrupt-checkpoint handling that fsync makes rare rather than
/// routine), the torn-checkpoint startup failure, and the retention-wedge
/// configuration guard.
/// </summary>
[TestClass]
public sealed class DurabilityTests
{
    public TestContext TestContext { get; set; } = null!;

    private static string CheckpointPath(string storageDir, string subscriptionId, string topic, int partition) =>
        Path.Combine(
            storageDir,
            "subscriptions",
            subscriptionId,
            topic,
            partition.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + ".checkpoint");

    [TestMethod]
    public async Task GetSubscriptionState_EmptyCheckpointFile_ThrowsCorruptCheckpointException()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            await consumer.CommitAsync(records[0].Position);
        }

        var path = CheckpointPath(dir, "archive", "agent.output", 0);
        Assert.IsTrue(File.Exists(path), "the commit above must have written a checkpoint");
        File.WriteAllText(path, string.Empty);

        await using var restarted = await TestHost.StartAsync(TestHost.Options(dir));
        var exception = Assert.ThrowsExactly<CorruptCheckpointException>(
            () => restarted.GetSubscriptionState("archive", "agent.output", 0));

        Assert.AreEqual("archive", exception.SubscriptionId);
        Assert.AreEqual("agent.output", exception.Topic);
        Assert.AreEqual(0, exception.Partition);
        Assert.AreEqual(path, exception.Path);
        Assert.IsInstanceOfType<System.Text.Json.JsonException>(exception.InnerException);
    }

    [TestMethod]
    public async Task GetSubscriptionState_TruncatedCheckpointFile_ThrowsCorruptCheckpointException()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            await consumer.CommitAsync(records[0].Position);
        }

        var path = CheckpointPath(dir, "archive", "agent.output", 0);
        var original = File.ReadAllText(path);
        File.WriteAllText(path, original[..(original.Length / 2)]);

        await using var restarted = await TestHost.StartAsync(TestHost.Options(dir));
        Assert.ThrowsExactly<CorruptCheckpointException>(
            () => restarted.GetSubscriptionState("archive", "agent.output", 0));
    }

    [TestMethod]
    public async Task RegisterConfiguredSubscriptions_CorruptCheckpoint_EscapesStartAsync()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            await consumer.CommitAsync(records[0].Position);
        }

        var path = CheckpointPath(dir, "archive", "agent.output", 0);
        File.WriteAllText(path, string.Empty);

        var options = TestHost.Options(dir);
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "archive",
            Topic = "agent.output",
            StartPosition = StartPosition.Latest,
        });

        var restarted = new LocalStreamHost(options);
        await Assert.ThrowsExactlyAsync<CorruptCheckpointException>(() => restarted.StartAsync());
    }

    [TestMethod]
    public async Task GetSubscriptionState_RepairedAfterCorruption_SucceedsOnRetry()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();
        await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));
        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[0].Position);

        var path = CheckpointPath(dir, "archive", "agent.output", 0);
        var original = File.ReadAllText(path);

        // Corrupt it behind the store's back (the in-process cache already
        // holds the good value from CommitAsync above, so force a fresh read
        // by using a distinct subscription id that has never been cached).
        var corruptPath = CheckpointPath(dir, "archive2", "agent.output", 0);
        Directory.CreateDirectory(Path.GetDirectoryName(corruptPath)!);
        File.WriteAllText(corruptPath, string.Empty);

        Assert.ThrowsExactly<CorruptCheckpointException>(
            () => host.GetSubscriptionState("archive2", "agent.output", 0));

        // A failed GetOrAdd factory does not cache the key, so repairing the
        // file and retrying must recover without restarting the host.
        File.WriteAllText(corruptPath, original);
        var state = host.GetSubscriptionState("archive2", "agent.output", 0);
        Assert.AreEqual(SubscriptionState.Active, state);
    }

    [TestMethod]
    public async Task StartAsync_RejectsTopicWhereMaxTotalBytesCannotHoldOneSegmentPerPartition()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            Partitions = 2,
            MaxSegmentBytes = 1024,
            MaxTotalBytes = 1023, // one byte short of 2 * 1024
        });

        var host = new LocalStreamHost(options);
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => host.StartAsync());
        StringAssert.Contains(exception.Message, "agent.output");
        StringAssert.Contains(exception.Message, "2048");
    }

    [TestMethod]
    public async Task StartAsync_AcceptsTopicAtExactlyPartitionsTimesMaxSegmentBytes()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            Partitions = 2,
            MaxSegmentBytes = 1024,
            MaxTotalBytes = 2048, // exactly 2 * 1024 — the boundary must be accepted
        });

        await using var host = new LocalStreamHost(options);
        await host.StartAsync();
    }

    [TestMethod]
    public async Task StartAsync_RejectsNonPositiveMaxSegmentBytes()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions { Name = "agent.output", MaxSegmentBytes = 0 });

        var host = new LocalStreamHost(options);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => host.StartAsync());
    }
}
