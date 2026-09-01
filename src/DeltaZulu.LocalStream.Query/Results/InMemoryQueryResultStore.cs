using System.Runtime.CompilerServices;
using DeltaZulu.LocalStream.Query.State;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Thread-safe reference materializer with logical change deduplication.</summary>
public sealed class InMemoryQueryResultStore : IQueryResultStore
{
    private readonly object _sync = new();
    private readonly Dictionary<StateDomainId, DomainResults> _domains = [];

    public ValueTask<MaterializationOutcome> ApplyAsync(
        StateDomainId domain,
        QueryChange change,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(change);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            var results = GetOrCreate(domain);
            if (results.AppliedChanges.Contains(change.ChangeId))
            {
                return ValueTask.FromResult(MaterializationOutcome.Duplicate);
            }

            results.Rows.TryGetValue(change.Key, out var current);
            if (current is not null && change.Version < current.Version)
            {
                results.AppliedChanges.Add(change.ChangeId);
                return ValueTask.FromResult(MaterializationOutcome.Stale);
            }

            if (current is not null && change.Version == current.Version)
            {
                throw new InvalidOperationException(
                    $"Result key '{change.Key.CanonicalKey}' received two identities for version {change.Version}.");
            }

            if (current?.IsFinalized == true)
            {
                throw new InvalidOperationException($"Finalized result key '{change.Key.CanonicalKey}' cannot be changed.");
            }

            var materialized = ApplyChange(current, change);
            results.Rows[change.Key] = materialized;
            results.AppliedChanges.Add(change.ChangeId);
            return ValueTask.FromResult(MaterializationOutcome.Applied);
        }
    }

    public ValueTask<MaterializedResult?> GetAsync(
        StateDomainId domain,
        ResultKey key,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            var result = _domains.TryGetValue(domain, out var results)
                && results.Rows.TryGetValue(key, out var row)
                    ? Clone(row)
                    : null;
            return ValueTask.FromResult(result);
        }
    }

    public async IAsyncEnumerable<MaterializedResult> ReadAllAsync(
        StateDomainId domain,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        MaterializedResult[] snapshot;
        lock (_sync)
        {
            snapshot = _domains.TryGetValue(domain, out var results)
                ? results.Rows.Values
                    .OrderBy(row => row.Key.CanonicalKey, StringComparer.Ordinal)
                    .ThenBy(row => row.Key.Window?.StartUtc)
                    .Select(Clone)
                    .ToArray()
                : [];
        }

        foreach (var row in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return row;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private DomainResults GetOrCreate(StateDomainId domain)
    {
        if (!_domains.TryGetValue(domain, out var results))
        {
            results = new DomainResults();
            _domains.Add(domain, results);
        }

        return results;
    }

    private static MaterializedResult ApplyChange(MaterializedResult? current, QueryChange change) =>
        change.Kind switch
        {
            QueryChangeKind.Upsert or QueryChangeKind.Correction => new MaterializedResult(
                change.Key, change.Version, change.Value!.Value.ToArray(), false, false, change.ChangeId),
            QueryChangeKind.Delete => new MaterializedResult(
                change.Key, change.Version, null, true, false, change.ChangeId),
            QueryChangeKind.Finalize when current is not null => current with
            {
                Version = change.Version,
                IsFinalized = true,
                LastChangeId = change.ChangeId,
            },
            QueryChangeKind.Finalize => throw new InvalidOperationException(
                $"Result key '{change.Key.CanonicalKey}' cannot be finalized before it exists."),
            _ => throw new ArgumentOutOfRangeException(nameof(change)),
        };

    private static MaterializedResult Clone(MaterializedResult result)
    {
        ReadOnlyMemory<byte>? value = result.Value.HasValue
            ? new ReadOnlyMemory<byte>(result.Value.Value.ToArray())
            : default(ReadOnlyMemory<byte>?);
        return result with { Value = value };
    }

    private sealed class DomainResults
    {
        public Dictionary<ResultKey, MaterializedResult> Rows { get; } = [];
        public HashSet<ResultChangeId> AppliedChanges { get; } = [];
    }
}
