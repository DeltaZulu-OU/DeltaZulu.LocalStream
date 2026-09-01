using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;
using LiteDB;
using LiteQuery = global::LiteDB.Query;

namespace DeltaZulu.LocalStream.Query.LiteDB;

/// <summary>LiteDB implementation of the transactional query-state protocol.</summary>
public sealed class LiteDbStateStore : IQueryStateStore, IDisposable
{
    private const string OperatorStateCollection = "operator_state";
    private const string CommittedRangeCollection = "committed_range";
    private const string OutputIntentCollection = "output_intent";
    private const string CheckpointCollection = "checkpoint_manifest";

    private readonly LiteDatabase _database;
    private readonly SemaphoreSlim _writer = new(1, 1);
    private bool _disposed;

    public LiteDbStateStore(string databasePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        var fullPath = Path.GetFullPath(databasePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        _database = new LiteDatabase(new ConnectionString { Filename = fullPath, Connection = ConnectionType.Direct });
        _database.GetCollection(OperatorStateCollection).EnsureIndex("domain");
        _database.GetCollection(OutputIntentCollection).EnsureIndex("domain");
    }

    public async ValueTask<IStateTransaction> BeginTransactionAsync(
        StateDomainId domain,
        SourceRange range,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(range);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_database.BeginTrans())
            {
                throw new InvalidOperationException("LiteDB refused to begin a query-state transaction.");
            }

            var domainKey = DomainKey(domain);
            var ranges = _database.GetCollection(CommittedRangeCollection);
            var overlap = ranges.FindOne(LiteQuery.And(
                LiteQuery.EQ("domain", domainKey),
                LiteQuery.EQ("topic", range.Topic),
                LiteQuery.EQ("partition", range.Partition),
                LiteQuery.LTE("start", range.EndOffset),
                LiteQuery.GTE("end", range.StartOffset)));
            var alreadyCommitted = overlap is not null
                && overlap["start"].AsInt64 == range.StartOffset
                && overlap["end"].AsInt64 == range.EndOffset;
            if (overlap is not null && !alreadyCommitted)
            {
                _database.Rollback();
                throw new InvalidOperationException("The source range overlaps an already committed range.");
            }

            return new LiteDbStateTransaction(this, domain, range, domainKey, alreadyCommitted);
        }
        catch
        {
            _database.Rollback();
            _writer.Release();
            throw;
        }
    }

    public async ValueTask<CheckpointManifest?> GetCheckpointAsync(
        StateDomainId domain,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var document = _database.GetCollection(CheckpointCollection).FindById(DomainKey(domain));
            return document is null ? null : ReadCheckpoint(domain, document);
        }
        finally
        {
            _writer.Release();
        }
    }

    public async IAsyncEnumerable<OutputIntent> ReadPendingOutputIntentsAsync(
        StateDomainId domain,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        BsonDocument[] documents;
        try
        {
            documents = _database.GetCollection(OutputIntentCollection)
                .Find(LiteQuery.EQ("domain", DomainKey(domain)))
                .OrderBy(document => document["changeId"].AsString, StringComparer.Ordinal)
                .ToArray();
        }
        finally
        {
            _writer.Release();
        }

        foreach (var document in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new OutputIntent(
                document["changeId"].AsString,
                document["target"].AsString,
                document["payload"].AsBinary.ToArray());
        }
    }

    public async ValueTask<bool> MarkOutputIntentDeliveredAsync(
        StateDomainId domain,
        string resultChangeId,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentException.ThrowIfNullOrWhiteSpace(resultChangeId);
        await _writer.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return _database.GetCollection(OutputIntentCollection)
                .Delete(Hash(DomainKey(domain), resultChangeId));
        }
        finally
        {
            _writer.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _database.Dispose();
        _writer.Dispose();
    }

    private void CompleteTransaction(bool commit)
    {
        try
        {
            if (commit)
            {
                _database.Commit();
            }
            else
            {
                _database.Rollback();
            }
        }
        finally
        {
            _writer.Release();
        }
    }

    private static string DomainKey(StateDomainId domain) => Hash(
        domain.QueryId,
        domain.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture),
        domain.PartitionAssignment);

    private static string StateDocumentId(string domain, StateKey key) => Hash(
        domain,
        key.OperatorId,
        key.Partition.ToString(System.Globalization.CultureInfo.InvariantCulture),
        key.LogicalKey,
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

    private static CheckpointManifest ReadCheckpoint(StateDomainId domain, BsonDocument document)
    {
        var range = new SourceRange(
            document["topic"].AsString,
            document["partition"].AsInt32,
            document["start"].AsInt64,
            document["end"].AsInt64);
        var watermark = document.TryGetValue("watermark", out var value) && value.IsBinary
            ? System.Text.Json.JsonSerializer.Deserialize<WatermarkState>(value.AsBinary)
            : null;
        return new CheckpointManifest(domain, range, document["cursor"].AsInt64, watermark, document["generation"].AsInt64);
    }

    private sealed class LiteDbStateTransaction(
        LiteDbStateStore store,
        StateDomainId domain,
        SourceRange sourceRange,
        string domainKey,
        bool isAlreadyCommitted) : IStateTransaction
    {
        private bool _completed;
        private WatermarkState? _watermark;
        private readonly Dictionary<StateKey, byte[]?> _changes = [];
        private readonly Dictionary<string, OutputIntent> _intents = new(StringComparer.Ordinal);

        public StateDomainId Domain { get; } = domain;
        public SourceRange SourceRange { get; } = sourceRange;
        public bool IsAlreadyCommitted { get; } = isAlreadyCommitted;

        public ValueTask<ReadOnlyMemory<byte>?> GetOperatorStateAsync(StateKey key, CancellationToken cancellationToken = default)
        {
            EnsureActive();
            cancellationToken.ThrowIfCancellationRequested();
            if (_changes.TryGetValue(key, out var staged))
            {
                return staged is null
                    ? ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)
                    : ValueTask.FromResult<ReadOnlyMemory<byte>?>(staged.AsMemory());
            }

            var document = store._database.GetCollection(OperatorStateCollection).FindById(StateDocumentId(domainKey, key));
            return document is null
                ? ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)
                : ValueTask.FromResult<ReadOnlyMemory<byte>?>(document["value"].AsBinary.ToArray());
        }

        public void PutOperatorState(StateKey key, ReadOnlyMemory<byte> value)
        {
            EnsureMutable();
            _changes[key] = value.ToArray();
        }

        public void DeleteOperatorState(StateKey key)
        {
            EnsureMutable();
            _changes[key] = null;
        }

        public void SetWatermark(WatermarkState state)
        {
            EnsureMutable();
            _watermark = state ?? throw new ArgumentNullException(nameof(state));
        }

        public void AddOutputIntent(OutputIntent intent)
        {
            EnsureMutable();
            ArgumentNullException.ThrowIfNull(intent);
            if (!_intents.TryAdd(intent.ResultChangeId, intent with { Payload = intent.Payload.ToArray() }))
            {
                throw new InvalidOperationException($"Output intent '{intent.ResultChangeId}' is duplicated.");
            }
        }

        public ValueTask<CheckpointManifest> CommitAsync(long candidateCursorOffset, CancellationToken cancellationToken = default)
        {
            EnsureActive();
            cancellationToken.ThrowIfCancellationRequested();
            ArgumentOutOfRangeException.ThrowIfNotEqual(candidateCursorOffset, SourceRange.EndOffset);

            if (!IsAlreadyCommitted)
            {
                PersistChanges();
            }

            var document = store._database.GetCollection(CheckpointCollection).FindById(domainKey);
            var checkpoint = ReadCheckpoint(Domain, document);
            _completed = true;
            store.CompleteTransaction(commit: true);
            return ValueTask.FromResult(checkpoint);
        }

        public ValueTask DisposeAsync()
        {
            if (!_completed)
            {
                _completed = true;
                store.CompleteTransaction(commit: false);
            }

            return ValueTask.CompletedTask;
        }

        private void PersistChanges()
        {
            var operatorState = store._database.GetCollection(OperatorStateCollection);
            foreach (var (key, value) in _changes)
            {
                var id = StateDocumentId(domainKey, key);
                if (value is null)
                {
                    operatorState.Delete(id);
                }
                else
                {
                    operatorState.Upsert(new BsonDocument { ["_id"] = id, ["domain"] = domainKey, ["value"] = value });
                }
            }

            var outputs = store._database.GetCollection(OutputIntentCollection);
            foreach (var intent in _intents.Values)
            {
                var id = Hash(domainKey, intent.ResultChangeId);
                var existing = outputs.FindById(id);
                if (existing is not null
                    && (existing["target"].AsString != intent.Target
                        || !existing["payload"].AsBinary.AsSpan().SequenceEqual(intent.Payload.Span)))
                {
                    throw new InvalidOperationException($"Output intent '{intent.ResultChangeId}' has conflicting content.");
                }

                outputs.Upsert(new BsonDocument
                {
                    ["_id"] = id, ["domain"] = domainKey, ["changeId"] = intent.ResultChangeId,
                    ["target"] = intent.Target, ["payload"] = intent.Payload.ToArray(),
                });
            }

            store._database.GetCollection(CommittedRangeCollection).Insert(new BsonDocument
            {
                ["_id"] = Hash(domainKey, SourceRange.Topic, SourceRange.Partition.ToString(), SourceRange.StartOffset.ToString(), SourceRange.EndOffset.ToString()),
                ["domain"] = domainKey, ["topic"] = SourceRange.Topic, ["partition"] = SourceRange.Partition,
                ["start"] = SourceRange.StartOffset, ["end"] = SourceRange.EndOffset,
            });

            var checkpoints = store._database.GetCollection(CheckpointCollection);
            var previous = checkpoints.FindById(domainKey);
            checkpoints.Upsert(new BsonDocument
            {
                ["_id"] = domainKey, ["topic"] = SourceRange.Topic, ["partition"] = SourceRange.Partition,
                ["start"] = SourceRange.StartOffset, ["end"] = SourceRange.EndOffset, ["cursor"] = SourceRange.EndOffset,
                ["generation"] = (previous?["generation"].AsInt64 ?? 0) + 1,
                ["watermark"] = _watermark is null
                    ? BsonValue.Null
                    : System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_watermark),
            });
        }

        private void EnsureActive()
        {
            if (_completed) throw new InvalidOperationException("The state transaction is already complete.");
        }

        private void EnsureMutable()
        {
            EnsureActive();
            if (IsAlreadyCommitted) throw new InvalidOperationException("An already committed range is read-only.");
        }
    }
}
