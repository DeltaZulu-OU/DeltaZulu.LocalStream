namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class StorageVerificationTests
{
    public TestContext TestContext { get; set; } = null!;

    private static async Task AppendAsync(LocalStreamHost host, int count)
    {
        var producer = host.CreateProducer<TestEvent>();
        for (var i = 0; i < count; i++)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
        }
    }

    [TestMethod]
    public async Task VerifyStorage_CleanStore_ReportsAllRecordsValid()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(
            TestHost.Options(dir, maxSegmentBytes: 512));
        await AppendAsync(host, 20);

        var report = host.VerifyStorage("agent.output");

        Assert.AreEqual("agent.output", report.Topic);
        Assert.IsTrue(report.IsClean);
        Assert.AreEqual(20, report.ValidRecords);
        Assert.IsTrue(report.SegmentsScanned > 1, "several segments must have been scanned");
        Assert.AreEqual(0, report.Partitions.Sum(p => p.TrailingGarbageBytes));
    }

    [TestMethod]
    public async Task VerifyStorage_TornTail_IsReportedButNotModified()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        await AppendAsync(host, 3);

        var segment = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Single();
        await File.AppendAllTextAsync(segment, "deadbeef {\"torn");
        var corruptedLength = new FileInfo(segment).Length;

        var report = host.VerifyStorage("agent.output");

        Assert.IsFalse(report.IsClean);
        Assert.AreEqual(3, report.ValidRecords);
        Assert.IsTrue(report.Partitions[0].TrailingGarbageBytes > 0);

        // Verification is read-only; repair stays a recovery/startup concern.
        Assert.AreEqual(corruptedLength, new FileInfo(segment).Length);
    }

    [TestMethod]
    public async Task VerifyStorage_CorruptedRecordInMiddle_ReportsIt()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(TestHost.Options(dir));
        await AppendAsync(host, 3);

        // Flip payload bytes of the middle record without touching framing,
        // so the line parses structurally but its CRC no longer matches.
        var segment = Directory
            .EnumerateFiles(dir, "*.log", SearchOption.AllDirectories)
            .Single();
        var content = await File.ReadAllTextAsync(segment);
        var corrupted = content.Replace("m1", "XX", StringComparison.Ordinal);
        Assert.AreNotEqual(content, corrupted, "test setup must corrupt something");
        await File.WriteAllTextAsync(segment, corrupted);

        var report = host.VerifyStorage("agent.output");

        Assert.IsFalse(report.IsClean);
        Assert.IsTrue(report.ValidRecords < 3);
        Assert.IsTrue(report.Partitions[0].TrailingGarbageBytes > 0);
    }
}
