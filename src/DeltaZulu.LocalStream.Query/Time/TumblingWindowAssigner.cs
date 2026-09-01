namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>
/// Assigns timestamps to fixed-size, non-overlapping windows aligned to the
/// Unix epoch. Assignment uses ticks exclusively and does not consult culture
/// or an ambient clock.
/// </summary>
public sealed class TumblingWindowAssigner : IWindowAssigner
{
    private static readonly long UnixEpochTicks = DateTimeOffset.UnixEpoch.UtcTicks;
    private readonly long _sizeTicks;

    public TumblingWindowAssigner(TimeSpan size)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(size, TimeSpan.Zero);
        Size = size;
        _sizeTicks = size.Ticks;
    }

    public TimeSpan Size { get; }

    public WindowInterval Assign(DateTimeOffset eventTimeUtc)
    {
        eventTimeUtc = eventTimeUtc.ToUniversalTime();
        var ticksFromEpoch = eventTimeUtc.UtcTicks - UnixEpochTicks;
        var bucket = FloorDivide(ticksFromEpoch, _sizeTicks);

        long startTicks;
        long endTicks;
        try
        {
            startTicks = checked(UnixEpochTicks + checked(bucket * _sizeTicks));
            endTicks = checked(startTicks + _sizeTicks);
        }
        catch (OverflowException)
        {
            throw OutsideRepresentableRange(eventTimeUtc);
        }

        if (startTicks < DateTimeOffset.MinValue.Ticks || endTicks > DateTimeOffset.MaxValue.Ticks)
        {
            throw OutsideRepresentableRange(eventTimeUtc);
        }

        return new WindowInterval(
            new DateTimeOffset(startTicks, TimeSpan.Zero),
            new DateTimeOffset(endTicks, TimeSpan.Zero));
    }

    private static long FloorDivide(long dividend, long divisor)
    {
        var quotient = Math.DivRem(dividend, divisor, out var remainder);
        return remainder < 0 ? quotient - 1 : quotient;
    }

    private ArgumentOutOfRangeException OutsideRepresentableRange(DateTimeOffset eventTimeUtc) =>
        new(
            nameof(eventTimeUtc),
            eventTimeUtc,
            $"The {Size} window containing this timestamp is outside the representable DateTimeOffset range.");
}
