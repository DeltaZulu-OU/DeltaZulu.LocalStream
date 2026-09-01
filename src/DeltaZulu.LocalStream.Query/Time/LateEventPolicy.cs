namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Allowed lateness and explicit handling for finalized-window events.</summary>
public sealed record LateEventPolicy
{
    public LateEventPolicy(TimeSpan allowedLateness, LateEventAction tooLateAction)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(allowedLateness, TimeSpan.Zero);
        if (!Enum.IsDefined(tooLateAction))
        {
            throw new ArgumentOutOfRangeException(nameof(tooLateAction), tooLateAction, "Unknown late-event action.");
        }

        AllowedLateness = allowedLateness;
        TooLateAction = tooLateAction;
    }

    public TimeSpan AllowedLateness { get; }

    public LateEventAction TooLateAction { get; }
}
