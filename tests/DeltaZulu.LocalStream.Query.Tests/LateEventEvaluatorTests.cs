using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class LateEventEvaluatorTests
{
    private static readonly DateTimeOffset Start =
        new(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);

    private static readonly WindowInterval Window = new(Start, Start.AddMinutes(5));

    [TestMethod]
    public void Evaluate_WithoutWatermark_IsOnTime()
    {
        var evaluator = CreateEvaluator();

        var decision = evaluator.Evaluate(new LateEventContext(Start, null, Window));

        Assert.AreEqual(LateEventDisposition.OnTime, decision.Disposition);
        Assert.IsTrue(decision.CanUpdateWindow);
        Assert.IsNull(decision.Action);
    }

    [TestMethod]
    public void Evaluate_EventAtWatermark_IsNotBehindWatermark()
    {
        var evaluator = CreateEvaluator();
        var watermark = Start.AddMinutes(2);

        var decision = evaluator.Evaluate(new LateEventContext(watermark, watermark, Window));

        Assert.AreEqual(LateEventDisposition.OnTime, decision.Disposition);
    }

    [TestMethod]
    public void Evaluate_BehindWatermarkWithinAllowedLateness_CanCorrectWindow()
    {
        var evaluator = CreateEvaluator();

        var decision = evaluator.Evaluate(new LateEventContext(
            Start.AddMinutes(1),
            Start.AddMinutes(5).AddSeconds(30),
            Window));

        Assert.AreEqual(LateEventDisposition.LateAccepted, decision.Disposition);
        Assert.IsTrue(decision.CanUpdateWindow);
        Assert.IsNull(decision.Action);
    }

    [TestMethod]
    public void Evaluate_AtFinalizationBoundary_IsTooLateWithConfiguredAction()
    {
        var evaluator = CreateEvaluator();
        var finalization = Window.EndUtc.AddMinutes(1);

        var before = evaluator.Evaluate(new LateEventContext(Start, finalization.AddTicks(-1), Window));
        var atBoundary = evaluator.Evaluate(new LateEventContext(Start, finalization, Window));

        Assert.AreEqual(LateEventDisposition.LateAccepted, before.Disposition);
        Assert.AreEqual(LateEventDisposition.TooLate, atBoundary.Disposition);
        Assert.IsFalse(atBoundary.CanUpdateWindow);
        Assert.AreEqual(LateEventAction.SideOutput, atBoundary.Action);
    }

    [TestMethod]
    public void IsFinalized_DoesNotOverflowAtMaximumTimestamp()
    {
        var evaluator = new LateEventEvaluator(
            new LateEventPolicy(TimeSpan.FromTicks(2), LateEventAction.Drop));
        var window = new WindowInterval(
            DateTimeOffset.MaxValue.AddTicks(-2),
            DateTimeOffset.MaxValue.AddTicks(-1));

        Assert.IsFalse(evaluator.IsFinalized(window, DateTimeOffset.MaxValue));
    }

    [TestMethod]
    public void Policy_RejectsInvalidValues()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new LateEventPolicy(TimeSpan.FromTicks(-1), LateEventAction.Drop));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new LateEventPolicy(TimeSpan.Zero, (LateEventAction)99));
    }

    private static LateEventEvaluator CreateEvaluator() => new(
        new LateEventPolicy(TimeSpan.FromMinutes(1), LateEventAction.SideOutput));
}
