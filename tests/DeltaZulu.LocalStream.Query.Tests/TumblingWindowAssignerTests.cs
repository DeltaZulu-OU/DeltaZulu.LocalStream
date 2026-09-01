using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class TumblingWindowAssignerTests
{
    private static readonly DateTimeOffset WindowStart =
        new(2026, 9, 1, 12, 30, 0, TimeSpan.Zero);

    [TestMethod]
    public void Assign_UsesStartInclusiveEndExclusiveBoundaries()
    {
        var assigner = new TumblingWindowAssigner(TimeSpan.FromMinutes(5));

        var atStart = assigner.Assign(WindowStart);
        var beforeEnd = assigner.Assign(WindowStart.AddMinutes(5).AddTicks(-1));
        var atEnd = assigner.Assign(WindowStart.AddMinutes(5));

        Assert.AreEqual(new WindowInterval(WindowStart, WindowStart.AddMinutes(5)), atStart);
        Assert.AreEqual(atStart, beforeEnd);
        Assert.AreEqual(
            new WindowInterval(WindowStart.AddMinutes(5), WindowStart.AddMinutes(10)),
            atEnd);
        Assert.IsTrue(atStart.Contains(WindowStart));
        Assert.IsFalse(atStart.Contains(WindowStart.AddMinutes(5)));
    }

    [TestMethod]
    public void Assign_NormalizesOffsetAndAlignsBeforeUnixEpoch()
    {
        var assigner = new TumblingWindowAssigner(TimeSpan.FromMinutes(5));
        var offsetTimestamp = new DateTimeOffset(1970, 1, 1, 1, 59, 0, TimeSpan.FromHours(2));

        var window = assigner.Assign(offsetTimestamp);

        Assert.AreEqual(new DateTimeOffset(1969, 12, 31, 23, 55, 0, TimeSpan.Zero), window.StartUtc);
        Assert.AreEqual(DateTimeOffset.UnixEpoch, window.EndUtc);
    }

    [TestMethod]
    public void Assign_IsStableAcrossRepeatedEvaluation()
    {
        var assigner = new TumblingWindowAssigner(TimeSpan.FromTicks(37));
        var timestamp = WindowStart.AddTicks(12345);

        Assert.AreEqual(assigner.Assign(timestamp), assigner.Assign(timestamp));
    }

    [TestMethod]
    public void Constructor_RejectsNonPositiveSize()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => new TumblingWindowAssigner(TimeSpan.Zero));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => new TumblingWindowAssigner(TimeSpan.FromTicks(-1)));
    }

    [TestMethod]
    public void Assign_RejectsWindowOutsideRepresentableTimestampRange()
    {
        var assigner = new TumblingWindowAssigner(TimeSpan.FromDays(1));

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => assigner.Assign(DateTimeOffset.MaxValue));
    }
}
