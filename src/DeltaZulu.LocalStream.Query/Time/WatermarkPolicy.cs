namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Deterministic event-time watermark configuration.</summary>
public sealed record WatermarkPolicy
{
    public WatermarkPolicy(TimeSpan outOfOrderness, TimeSpan idleTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(outOfOrderness, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(idleTimeout, TimeSpan.Zero);

        OutOfOrderness = outOfOrderness;
        IdleTimeout = idleTimeout;
    }

    public TimeSpan OutOfOrderness { get; }

    public TimeSpan IdleTimeout { get; }
}
