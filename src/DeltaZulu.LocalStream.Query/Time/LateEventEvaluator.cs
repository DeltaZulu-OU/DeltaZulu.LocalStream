namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>
/// Separates events behind the watermark from events whose windows have
/// finalized. No ambient time is consulted.
/// </summary>
public sealed class LateEventEvaluator(LateEventPolicy policy)
{
    private readonly LateEventPolicy _policy =
        policy ?? throw new ArgumentNullException(nameof(policy));

    public LateEventDecision Evaluate(LateEventContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var eventTimeUtc = context.EventTimeUtc.ToUniversalTime();
        var watermarkUtc = context.WatermarkUtc?.ToUniversalTime();

        if (watermarkUtc is null)
        {
            return new LateEventDecision(LateEventDisposition.OnTime, null);
        }

        if (IsFinalized(context.Window, watermarkUtc.Value))
        {
            return new LateEventDecision(LateEventDisposition.TooLate, _policy.TooLateAction);
        }

        return eventTimeUtc < watermarkUtc
            ? new LateEventDecision(LateEventDisposition.LateAccepted, null)
            : new LateEventDecision(LateEventDisposition.OnTime, null);
    }

    public bool IsFinalized(WindowInterval window, DateTimeOffset watermarkUtc)
    {
        watermarkUtc = watermarkUtc.ToUniversalTime();
        var remainingTicks = DateTimeOffset.MaxValue.UtcTicks - window.EndUtc.UtcTicks;
        if (_policy.AllowedLateness.Ticks > remainingTicks)
        {
            return false;
        }

        var finalizationTicks = window.EndUtc.UtcTicks + _policy.AllowedLateness.Ticks;
        return watermarkUtc.UtcTicks >= finalizationTicks;
    }
}
