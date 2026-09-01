using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;
using LiteDB;
using LiteQuery = global::LiteDB.Query;

namespace DeltaZulu.LocalStream.Query.LiteDB;

/// <summary>Durable LiteDB materializer for deterministic query changelogs.</summary>
public sealed class LiteDbQueryResultStore : IQueryResultStore, IDisposable
{
    private const string ResultsCollection = "materialized_result";
    private const string AppliedChangesCollection = "applied_result_change";
    private readonly LiteDatabase _database;
    private readonly SemaphoreSlim _writer = new(1, 1);
    private bool _disposed;

    public LiteDbQueryResultStore(string databasePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        var fullPath = Path.GetFullPath(databasePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        _database = new LiteDatabase(new ConnectionString { Filename = fullPath, Connection = ConnectionType.Direct });
        _database.GetCollection(ResultsCollection).EnsureIndex("domain");
        _database.GetCollection(AppliedChangesCollection).EnsureIndex("domain");
    }

    public async ValueTask<MaterializationOutcome> ApplyAsync(
        StateDomainId domain,
        QueryChange change,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(change);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        var transactionStarted = false;
        try
        {
            transactionStarted = _database.BeginTrans();
            if (!transactionStarted)
            {
                throw new InvalidOperationException("LiteDB refused to begin a result materialization transaction.");
            }

            var domainKey = DomainKey(domain);
            var applied = _database.GetCollection(AppliedChangesCollection);
            var appliedId = Hash(domainKey, change.ChangeId.Value);
            if (applied.Exists(LiteQuery.EQ("_id", appliedId)))
            {
                _database.Rollback();
                transactionStarted = false;
                return MaterializationOutcome.Duplicate;
            }

            var rows = _database.GetCollection(ResultsCollection);
            var rowId = RowId(domainKey, change.Key);
            var currentDocument = rows.FindById(rowId);
            var current = currentDocument is null ? null : ReadResult(currentDocument);
            if (current is not null && change.Version < current.Version)
            {
                applied.Insert(AppliedDocument(appliedId, domainKey, change.ChangeId));
                _database.Commit();
                transactionStarted = false;
                return MaterializationOutcome.Stale;
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
            rows.Upsert(WriteResult(rowId, domainKey, materialized));
            applied.Insert(AppliedDocument(appliedId, domainKey, change.ChangeId));
            _database.Commit();
            transactionStarted = false;
            return MaterializationOutcome.Applied;
        }
        catch
        {
            if (transactionStarted)
            {
                _database.Rollback();
            }

            throw;
        }
        finally
        {
            _writer.Release();
        }
    }

    public async ValueTask<MaterializedResult?> GetAsync(
        StateDomainId domain,
        ResultKey key,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(key);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var document = _database.GetCollection(ResultsCollection).FindById(RowId(DomainKey(domain), key));
            return document is null ? null : ReadResult(document);
        }
        finally
        {
            _writer.Release();
        }
    }

    public async IAsyncEnumerable<MaterializedResult> ReadAllAsync(
        StateDomainId domain,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        MaterializedResult[] snapshot;
        try
        {
            snapshot = _database.GetCollection(ResultsCollection)
                .Find(LiteQuery.EQ("domain", DomainKey(domain)))
                .Select(ReadResult)
                .OrderBy(result => result.Key.CanonicalKey, StringComparer.Ordinal)
                .ThenBy(result => result.Key.Window?.StartUtc)
                .ToArray();
        }
        finally
        {
            _writer.Release();
        }

        foreach (var result in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _database.Dispose();
        _writer.Dispose();
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

    private static BsonDocument AppliedDocument(string id, string domain, ResultChangeId changeId) => new()
    {
        ["_id"] = id,
        ["domain"] = domain,
        ["changeId"] = changeId.Value,
    };

    private static BsonDocument WriteResult(string id, string domain, MaterializedResult result)
    {
        var document = new BsonDocument
        {
            ["_id"] = id,
            ["domain"] = domain,
            ["key"] = result.Key.CanonicalKey,
            ["version"] = result.Version,
            ["deleted"] = result.IsDeleted,
            ["finalized"] = result.IsFinalized,
            ["lastChangeId"] = result.LastChangeId.Value,
            ["value"] = result.Value.HasValue ? result.Value.Value.ToArray() : BsonValue.Null,
            ["hasWindow"] = result.Key.Window.HasValue,
        };
        if (result.Key.Window is { } window)
        {
            document["windowStartTicks"] = window.StartUtc.UtcTicks;
            document["windowEndTicks"] = window.EndUtc.UtcTicks;
        }

        return document;
    }

    private static MaterializedResult ReadResult(BsonDocument document)
    {
        WindowInterval? window = document["hasWindow"].AsBoolean
            ? new WindowInterval(
                new DateTimeOffset(document["windowStartTicks"].AsInt64, TimeSpan.Zero),
                new DateTimeOffset(document["windowEndTicks"].AsInt64, TimeSpan.Zero))
            : null;
        ReadOnlyMemory<byte>? value = document["value"].IsBinary
            ? new ReadOnlyMemory<byte>(document["value"].AsBinary.ToArray())
            : default(ReadOnlyMemory<byte>?);
        return new MaterializedResult(
            new ResultKey(document["key"].AsString, window),
            document["version"].AsInt64,
            value,
            document["deleted"].AsBoolean,
            document["finalized"].AsBoolean,
            ResultChangeId.Parse(document["lastChangeId"].AsString));
    }

    private static string DomainKey(StateDomainId domain) => Hash(
        domain.QueryId,
        domain.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture),
        domain.PartitionAssignment);

    private static string RowId(string domain, ResultKey key) => Hash(
        domain,
        key.CanonicalKey,
        key.Window?.StartUtc.UtcTicks.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
        key.Window?.EndUtc.UtcTicks.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty);

    private static string Hash(params string[] values)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            foreach (var value in values)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                writer.Write(bytes.Length);
                writer.Write(bytes);
            }
        }

        return Convert.ToHexString(SHA256.HashData(stream.GetBuffer().AsSpan(0, checked((int)stream.Length))));
    }
}
