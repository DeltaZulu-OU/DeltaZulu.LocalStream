namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>A start-inclusive, end-exclusive UTC event-time interval.</summary>
public readonly record struct WindowInterval
{
    public WindowInterval(DateTimeOffset startUtc, DateTimeOffset endUtc)
    {
        startUtc = startUtc.ToUniversalTime();
        endUtc = endUtc.ToUniversalTime();
        if (endUtc <= startUtc)
        {
            throw new ArgumentOutOfRangeException(
                nameof(endUtc),
                endUtc,
                "A window end must be later than its start.");
        }

        StartUtc = startUtc;
        EndUtc = endUtc;
    }

    public DateTimeOffset StartUtc { get; }

    public DateTimeOffset EndUtc { get; }

    public bool Contains(DateTimeOffset eventTimeUtc)
    {
        eventTimeUtc = eventTimeUtc.ToUniversalTime();
        return eventTimeUtc >= StartUtc && eventTimeUtc < EndUtc;
    }
}
