using DeltaZulu.LocalStream.Query.State;

namespace DeltaZulu.LocalStream.Query.Results;

public interface IQueryResultSink
{
    ValueTask<MaterializationOutcome> ApplyAsync(
        StateDomainId domain,
        QueryChange change,
        CancellationToken cancellationToken = default);
}

public interface IQueryResultStore : IQueryResultSink
{
    ValueTask<MaterializedResult?> GetAsync(
        StateDomainId domain,
        ResultKey key,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MaterializedResult> ReadAllAsync(
        StateDomainId domain,
        CancellationToken cancellationToken = default);
}
