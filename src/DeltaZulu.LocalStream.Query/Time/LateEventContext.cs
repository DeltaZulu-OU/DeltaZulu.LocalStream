namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Deterministic inputs used to classify one event for one window.</summary>
public sealed record LateEventContext(
    DateTimeOffset EventTimeUtc,
    DateTimeOffset? WatermarkUtc,
    WindowInterval Window);

/// <summary>Classification result and any required terminal action.</summary>
public sealed record LateEventDecision(
    LateEventDisposition Disposition,
    LateEventAction? Action)
{
    public bool CanUpdateWindow => Disposition is not LateEventDisposition.TooLate;
}
