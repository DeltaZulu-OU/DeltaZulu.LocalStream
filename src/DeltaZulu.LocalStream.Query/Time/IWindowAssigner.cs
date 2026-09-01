namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Assigns an event timestamp to a deterministic event-time window.</summary>
public interface IWindowAssigner
{
    WindowInterval Assign(DateTimeOffset eventTimeUtc);
}
