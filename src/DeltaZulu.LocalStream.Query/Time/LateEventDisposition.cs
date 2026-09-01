namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Classification of an event relative to watermark and window state.</summary>
public enum LateEventDisposition
{
    OnTime,
    LateAccepted,
    TooLate,
}
