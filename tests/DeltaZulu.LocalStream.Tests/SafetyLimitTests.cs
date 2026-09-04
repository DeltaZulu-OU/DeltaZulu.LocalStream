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
    public async Task Append_BeyondMaxTotalBytes_NeverExceedsCapOnDisk()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        const long MaxTotalBytes = 2048;
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            MaxSegmentBytes = 512,
            MaxTotalBytes = MaxTotalBytes,
            Retention = new RetentionOptions { MaxBytes = 1024 },
        });
        await using var host = await TestHost.StartAsync(options);
        var producer = host.CreateProducer<TestEvent>();

        var rejectedCount = 0;
        for (var i = 0; i < 200 && rejectedCount < 3; i++)
        {
            var result = await producer.AppendAsync(
                "agent.output",
                new TestEvent("s", $"padded-message-{i:D6}"));
            if (result.Status == AppendStatus.RejectedStreamFull)
            {
                rejectedCount++;
            }

            // The old pre-append check (TotalSizeBytes >= cap) let the append
            // that crossed the boundary through; the strict check must not.
            Assert.IsTrue(
                host.GetTopicMetrics("agent.output").SizeBytes <= MaxTotalBytes,
                $"on-disk size exceeded the {MaxTotalBytes}-byte cap after append {i}");
        }

        Assert.IsTrue(rejectedCount > 0, "the hard cap must eventually reject appends");
    }

    [TestMethod]
    public async Task AppendBatchAsync_StraddlingCap_AdmitsUpToCap_RejectsTheRest()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        const long MaxTotalBytes = 1024;
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            MaxSegmentBytes = MaxTotalBytes,
            MaxTotalBytes = MaxTotalBytes,
        });
        await using var host = await TestHost.StartAsync(options);
        var producer = host.CreateProducer<TestEvent>();

        var batch = Enumerable.Range(0, 50)
            .Select(i => new TestEvent("s", $"padded-message-{i:D6}"))
            .ToList();
        var results = await producer.AppendBatchAsync("agent.output", batch);

        Assert.IsTrue(results.Any(r => r.Status == AppendStatus.Appended), "some records must fit");
        Assert.IsTrue(results.Any(r => r.Status == AppendStatus.RejectedStreamFull), "the batch must straddle the cap");
        Assert.IsTrue(
            host.GetTopicMetrics("agent.output").SizeBytes <= MaxTotalBytes,
            "a single batch must not push the topic over its cap");
    }

    [TestMethod]
    public async Task ConcurrentAppends_AgainstSmallCap_DoNotJointlyExceedIt()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = new LocalStreamOptions { StoragePath = dir };
        const long MaxTotalBytes = 4096;
        options.Topics.Add(new TopicOptions
        {
            Name = "agent.output",
            MaxSegmentBytes = MaxTotalBytes,
            MaxTotalBytes = MaxTotalBytes,
        });
        await using var host = await TestHost.StartAsync(options);
        var producer = host.CreateProducer<TestEvent>();

        // Concurrent producers race TopicLog.TotalSizeBytes's cache; without a
        // lock around the check-compute-store, a stale read lets more than one
        // writer through past the cap at once.
        var tasks = Enumerable.Range(0, 16)
            .Select(i => Task.Run(async () =>
            {
                var appended = 0;
                for (var j = 0; j < 20; j++)
                {
                    var result = await producer.AppendAsync(
                        "agent.output", new TestEvent("s", $"worker-{i}-{j:D4}"));
                    if (result.Status == AppendStatus.Appended)
                    {
                        appended++;
                    }
                }

                return appended;
            }))
            .ToArray();
        await Task.WhenAll(tasks);

        Assert.IsTrue(
            host.GetTopicMetrics("agent.output").SizeBytes <= MaxTotalBytes,
            "concurrent producers must not jointly overshoot the cap");
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
