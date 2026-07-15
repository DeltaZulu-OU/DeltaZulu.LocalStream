namespace DeltaZulu.LocalStream.Tests;

/// <summary>
/// Unix file-mode hardening from the architecture doc's safety baseline:
/// stream data must be owner-only, matching DurableBuffer's approach.
/// </summary>
[TestClass]
public sealed class FilePermissionTests
{
    public TestContext TestContext { get; set; } = null!;

    private static void AssertOwnerOnly(string path)
    {
        var mode = File.GetUnixFileMode(path);
        const UnixFileMode NonOwner =
            UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

        Assert.AreEqual(
            UnixFileMode.None,
            mode & NonOwner,
            $"'{path}' must be owner-only but has mode {mode}");
    }

    [TestMethod]
    public async Task SegmentCheckpointAndMetadataFiles_AreOwnerOnly_OnUnix()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Inconclusive("Unix file modes do not apply on Windows.");
        }

        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));

        var producer = host.CreateProducer<TestEvent>();
        await producer.AppendAsync("agent.output", new TestEvent("s", "m0"));

        var consumer = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(consumer, "agent.output");
        await consumer.CommitAsync(records[0].Position);

        var segment = Directory.EnumerateFiles(dir, "*.log", SearchOption.AllDirectories).Single();
        var checkpoint = Directory.EnumerateFiles(dir, "*.checkpoint", SearchOption.AllDirectories).Single();
        var topicsMetadata = Path.Combine(dir, "metadata", "topics.json");

        AssertOwnerOnly(segment);
        AssertOwnerOnly(checkpoint);
        AssertOwnerOnly(topicsMetadata);
    }

    [TestMethod]
    public async Task BatchAppendedSegments_AreOwnerOnly_OnUnix()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Inconclusive("Unix file modes do not apply on Windows.");
        }

        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(
            TestHost.Options(dir, maxSegmentBytes: 512));

        var producer = host.CreateProducer<TestEvent>();
        await producer.AppendBatchAsync(
            "agent.output",
            Enumerable.Range(0, 30).Select(i => new TestEvent("s", $"padded-message-{i:D6}")).ToList());

        var segments = Directory.EnumerateFiles(dir, "*.log", SearchOption.AllDirectories).ToList();
        Assert.IsTrue(segments.Count > 1);
        foreach (var segment in segments)
        {
            AssertOwnerOnly(segment);
        }
    }
}
