namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Semantic operation carried by a continuous-query changelog.</summary>
public enum QueryChangeKind
{
    Upsert,
    Delete,
    Correction,
    Finalize,
}
