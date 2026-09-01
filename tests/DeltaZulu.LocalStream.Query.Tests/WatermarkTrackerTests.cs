using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class WatermarkTrackerTests
{
    private static readonly DateTimeOffset Epoch = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);

    [TestMethod]
    public void Watermark_IsMinimumActivePartitionProgressMinusOutOfOrderness()
    {
        var tracker = CreateTracker();

        Assert.AreEqual(Epoch.AddMinutes(-1), tracker.Observe(0, Epoch, Epoch));
        Assert.AreEqual(Epoch.AddMinutes(-1), tracker.Observe(1, Epoch, Epoch));
        Assert.AreEqual(Epoch.AddMinutes(-1), tracker.Observe(0, Epoch.AddMinutes(2), Epoch.AddSeconds(1)));
        Assert.AreEqual(Epoch.AddMinutes(1), tracker.Observe(1, Epoch.AddMinutes(5), Epoch.AddSeconds(2)));
    }

    [TestMethod]
    public void IdlePartition_IsExcludedButAllIdleRetainsLastWatermark()
    {
        var tracker = CreateTracker();
        tracker.Observe(0, Epoch, Epoch);
        tracker.Observe(1, Epoch.AddMinutes(5), Epoch.AddMinutes(9));

        Assert.AreEqual(Epoch.AddMinutes(-1), tracker.WatermarkUtc);
        Assert.AreEqual(Epoch.AddMinutes(4), tracker.Advance(Epoch.AddMinutes(10)));
        Assert.AreEqual(Epoch.AddMinutes(4), tracker.WatermarkUtc);
        Assert.AreEqual(Epoch.AddMinutes(4), tracker.Advance(Epoch.AddMinutes(20)));
    }

    [TestMethod]
    public void LateAndReactivatedPartition_NeverMovesWatermarkBackward()
    {
        var tracker = CreateTracker();
        tracker.Observe(0, Epoch.AddMinutes(10), Epoch);
        tracker.Advance(Epoch.AddMinutes(11));

        Assert.AreEqual(Epoch.AddMinutes(9), tracker.WatermarkUtc);
        Assert.AreEqual(Epoch.AddMinutes(9), tracker.Observe(1, Epoch, Epoch.AddMinutes(12)));
        Assert.AreEqual(Epoch.AddMinutes(9), tracker.Observe(0, Epoch.AddMinutes(2), Epoch.AddMinutes(13)));
    }

    [TestMethod]
    public void RestoredState_PreservesPartitionMaximaAndPublishedWatermark()
    {
        var original = CreateTracker();
        original.Observe(1, Epoch.AddMinutes(8), Epoch);
        original.Observe(0, Epoch.AddMinutes(10), Epoch);

        var restored = new WatermarkTracker(
            new WatermarkPolicy(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10)),
            original.CaptureState());

        Assert.AreEqual(original.WatermarkUtc, restored.WatermarkUtc);
        CollectionAssert.AreEqual(
            new[] { 0, 1 },
            restored.CaptureState().Partitions.Select(state => state.Partition).ToArray());
        Assert.AreEqual(Epoch.AddMinutes(7), restored.Advance(Epoch.AddMinutes(1)));
    }

    [TestMethod]
    public void Policy_RejectsInvalidDurations()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => new WatermarkPolicy(TimeSpan.FromTicks(-1), TimeSpan.FromSeconds(1)));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => new WatermarkPolicy(TimeSpan.Zero, TimeSpan.Zero));
    }

    private static WatermarkTracker CreateTracker() => new(
        new WatermarkPolicy(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10)));
}
