namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class SafetyLimitTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task Append_RecordLargerThanMaxRecordBytes_IsRejected()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            MaxRecordBytes = 256,
        });
        await using var host = await TestHost.StartAsync(options);
        var producer = host.CreateProducer<TestEvent>();

        var small = await producer.AppendAsync("agent.output", new TestEvent("s", "fits"));
        var huge = await producer.AppendAsync(
            "agent.output",
            new TestEvent("s", new string('x', 1024)));

        Assert.AreEqual(AppendStatus.Appended, small.Status);
        Assert.AreEqual(AppendStatus.RejectedRecordTooLarge, huge.Status);
        Assert.IsNull(huge.Position);
        Assert.IsNotNull(huge.Reason);

        // The rejected record consumed no offset.
        var next = await producer.AppendAsync("agent.output", new TestEvent("s", "next"));
        Assert.AreEqual(1, next.Position!.Offset);
    }

    [TestMethod]
    public async Task Append_BeyondMaxTotalBytes_IsRejectedUntilRetentionFreesSpace()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            MaxSegmentBytes = 512,
            MaxTotalBytes = 2048,
            Retention = new RetentionOptions { MaxBytes = 1024 },
        });
        await using var host = await TestHost.StartAsync(options);
        var producer = host.CreateProducer<TestEvent>();

        AppendResult? rejected = null;
        for (var i = 0; i < 200 && rejected is null; i++)
        {
            var result = await producer.AppendAsync(
                "agent.output",
                new TestEvent("s", $"padded-message-{i:D6}"));
            if (result.Status == AppendStatus.RejectedStreamFull)
            {
                rejected = result;
            }
            else
            {
                Assert.AreEqual(AppendStatus.Appended, result.Status);
            }
        }

        Assert.IsNotNull(rejected, "the hard cap must eventually reject appends");
        Assert.IsNull(rejected.Position);

        // Retention frees sealed segments; appends then succeed again.
        await host.ApplyRetentionAsync();
        var afterRetention = await producer.AppendAsync(
            "agent.output", new TestEvent("s", "after-retention"));
        Assert.AreEqual(AppendStatus.Appended, afterRetention.Status);
    }

    [TestMethod]
    public async Task Append_WithoutConfiguredLimits_IsUnbounded()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        var producer = host.CreateProducer<TestEvent>();

        var large = await producer.AppendAsync(
            "agent.output",
            new TestEvent("s", new string('x', 64 * 1024)));

        Assert.AreEqual(AppendStatus.Appended, large.Status);
    }
}
