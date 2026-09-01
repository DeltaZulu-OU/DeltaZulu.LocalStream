using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.State;

/// <summary>Atomic recovery point for a committed source range.</summary>
public sealed record CheckpointManifest(
    StateDomainId Domain,
    SourceRange SourceRange,
    long CursorOffset,
    WatermarkState? Watermark,
    long Generation);
