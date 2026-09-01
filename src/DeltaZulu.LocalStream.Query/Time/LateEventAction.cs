namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Configured handling for an event whose window is already final.</summary>
public enum LateEventAction
{
    Drop,
    SideOutput,
}
