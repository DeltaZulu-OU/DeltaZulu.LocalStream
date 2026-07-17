using System.Buffers;
using System.Globalization;
using System.Text.Json;

namespace DeltaZulu.LocalStream.Storage;

/// <summary>
/// One append-only partition log made of rolling segment files. Each segment
/// line is <c>&lt;crc32-hex8&gt; &lt;envelope-json&gt;\n</c>; the CRC covers the JSON bytes.
/// Reading never deletes; only retention removes whole sealed segments.
/// </summary>
internal sealed class PartitionLog
{
    private const string SegmentExtension = ".log";
    private const int ReadBufferBytes = 64 * 1024;

    private readonly object _sync = new();
    private readonly string _segmentsDirectory;
    private readonly long _maxSegmentBytes;
    private readonly List<Segment> _segments = [];

    private long _nextOffset;

    private sealed class Segment(long baseOffset, string path)
    {
        public long BaseOffset { get; } = baseOffset;
        public string Path { get; } = path;
        public long SizeBytes { get; set; }
        public long RecordCount { get; set; }
        public DateTimeOffset NewestRecordUtc { get; set; }
    }

    public PartitionLog(string partitionDirectory, long maxSegmentBytes)
    {
        _segmentsDirectory = Path.Combine(partitionDirectory, "segments");
        _maxSegmentBytes = maxSegmentBytes;
        Directory.CreateDirectory(_segmentsDirectory);
        Recover();
    }

    public long NextOffset
    {
        get
        {
            lock (_sync)
            {
                return _nextOffset;
            }
        }
    }

    /// <summary>
    /// Offset of the oldest record still on disk. Equals <see cref="NextOffset"/>
    /// when the partition holds no records.
    /// </summary>
    public long EarliestRetainedOffset
    {
        get
        {
            lock (_sync)
            {
                var first = _segments.FirstOrDefault(s => s.RecordCount > 0);
                return first?.BaseOffset ?? _nextOffset;
            }
        }
    }

    /// <summary>
    /// Total on-disk bytes across all segments. O(segments), cached in a
    /// property to avoid repeated enumeration. If hot, consider caching
    /// in a field and invalidating on changes.
    /// </summary>
    public long SizeBytes
    {
        get
        {
            lock (_sync)
            {
                return _segments.Sum(s => s.SizeBytes);
            }
        }
    }

    public int SegmentCount
    {
        get
        {
            lock (_sync)
            {
                return _segments.Count;
            }
        }
    }

    internal readonly record struct PendingRecord(
        string EventId,
        DateTimeOffset PublishedUtc,
        IReadOnlyDictionary<string, string>? Headers,
        byte[] PayloadJson);

    public long Append(string eventId, DateTimeOffset publishedUtc, IReadOnlyDictionary<string, string>? headers, byte[] payloadJson) =>
        AppendMany([new PendingRecord(eventId, publishedUtc, headers, payloadJson)]);

    /// <summary>
    /// Appends records consecutively with one durable flush per touched
    /// segment. Returns the offset of the first record; the rest follow
    /// contiguously.
    /// </summary>
    public long AppendMany(IReadOnlyList<PendingRecord> records)
    {
        lock (_sync)
        {
            var firstOffset = _nextOffset;
            FileStream? stream = null;
            Segment? current = null;
            try
            {
                foreach (var record in records)
                {
                    var line = FrameLine(SerializeEnvelope(_nextOffset, record));

                    var segment = ActiveSegmentForWrite();
                    if (!ReferenceEquals(segment, current))
                    {
                        stream?.Flush(flushToDisk: true);
                        stream?.Dispose();
                        stream = new FileStream(segment.Path, FileMode.Append, FileAccess.Write, FileShare.Read);
                        current = segment;
                    }

                    stream!.Write(line);
                    segment.SizeBytes += line.Length;
                    segment.RecordCount++;
                    segment.NewestRecordUtc = record.PublishedUtc;
                    _nextOffset++;
                }

                stream?.Flush(flushToDisk: true);
            }
            finally
            {
                stream?.Dispose();
            }

            return firstOffset;
        }
    }

    public IEnumerable<RecordEnvelope> Read(long fromOffset)
    {
        List<Segment> snapshot;
        long endOffsetExclusive;
        lock (_sync)
        {
            snapshot = [.. _segments];
            endOffsetExclusive = _nextOffset;
        }

        foreach (var segment in snapshot)
        {
            if (segment.BaseOffset + segment.RecordCount <= fromOffset)
            {
                continue;
            }

            foreach (var envelope in ReadSegmentFrom(segment.Path, segment.BaseOffset, fromOffset, endOffsetExclusive))
            {
                yield return envelope;
            }
        }
    }

    /// <summary>Finds the first offset published at or after the given timestamp, or null.</summary>
    public long? FindOffsetByTimestamp(DateTimeOffset timestamp)
    {
        foreach (var envelope in Read(EarliestRetainedOffset))
        {
            if (envelope.PublishedUtc >= timestamp)
            {
                return envelope.Offset;
            }
        }

        return null;
    }

    /// <summary>
    /// Deletes oldest sealed segments violating the policy. The active (newest)
    /// segment is never deleted, so retention cannot outrun the write head.
    /// </summary>
    public void ApplyRetention(RetentionOptions retention, DateTimeOffset nowUtc)
    {
        lock (_sync)
        {
            var totalBytes = _segments.Sum(s => s.SizeBytes);
            while (_segments.Count > 1 && ViolatesPolicy(_segments, retention, nowUtc, totalBytes))
            {
                var oldest = _segments[0];
                totalBytes -= oldest.SizeBytes;
                File.Delete(oldest.Path);
                _segments.RemoveAt(0);
            }
        }
    }

    /// <summary>
    /// Dry run of <see cref="ApplyRetention"/>: reports what the same policy
    /// walk would delete, without deleting.
    /// </summary>
    public (int Segments, long Bytes, long Records, long FirstRetainedOffset) AuditRetention(
        RetentionOptions retention,
        DateTimeOffset nowUtc)
    {
        lock (_sync)
        {
            var remaining = new List<Segment>(_segments);
            var totalBytes = remaining.Sum(s => s.SizeBytes);
            var segments = 0;
            var bytes = 0L;
            var records = 0L;
            while (remaining.Count > 1 && ViolatesPolicy(remaining, retention, nowUtc, totalBytes))
            {
                var oldest = remaining[0];
                segments++;
                bytes += oldest.SizeBytes;
                records += oldest.RecordCount;
                totalBytes -= oldest.SizeBytes;
                remaining.RemoveAt(0);
            }

            var firstRetained = remaining.FirstOrDefault(s => s.RecordCount > 0)?.BaseOffset ?? _nextOffset;
            return (segments, bytes, records, firstRetained);
        }
    }

    private static bool ViolatesPolicy(
        List<Segment> segments,
        RetentionOptions retention,
        DateTimeOffset nowUtc,
        long totalBytes)
    {
        var oldest = segments[0];

        if (retention.MaxBytes is { } maxBytes && totalBytes > maxBytes)
        {
            return true;
        }

        if (retention.MaxAge is { } maxAge
            && oldest.RecordCount > 0
            && oldest.NewestRecordUtc < nowUtc - maxAge)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Read-only integrity scan: re-validates every segment line's CRC frame
    /// and reports bytes trailing the last valid record. Never modifies files.
    /// </summary>
    public (int Segments, long ValidRecords, long TrailingGarbageBytes) Verify()
    {
        List<Segment> snapshot;
        lock (_sync)
        {
            snapshot = [.. _segments];
        }

        var segments = 0;
        var validRecords = 0L;
        var garbageBytes = 0L;
        foreach (var segment in snapshot)
        {
            long fileLength;
            try
            {
                fileLength = new FileInfo(segment.Path).Length;
            }
            catch (FileNotFoundException)
            {
                // Deleted by retention between snapshot and scan.
                continue;
            }

            var scan = ScanSegment(segment.Path);
            if (scan is not { } result)
            {
                continue;
            }

            segments++;
            validRecords += result.RecordCount;
            garbageBytes += fileLength - result.ValidBytes;
        }

        return (segments, validRecords, garbageBytes);
    }

    private Segment ActiveSegmentForWrite()
    {
        var active = _segments.Count > 0 ? _segments[^1] : null;
        if (active is null || active.SizeBytes >= _maxSegmentBytes)
        {
            active = new Segment(_nextOffset, SegmentPath(_nextOffset));
            SafeFiles.CreateEmpty(active.Path);
            _segments.Add(active);
        }

        return active;
    }

    private string SegmentPath(long baseOffset) =>
        Path.Combine(_segmentsDirectory, baseOffset.ToString("D20", CultureInfo.InvariantCulture) + SegmentExtension);

    private void Recover()
    {
        var files = Directory
            .EnumerateFiles(_segmentsDirectory, "*" + SegmentExtension)
            .OrderBy(Path.GetFileName, StringComparer.Ordinal)
            .ToList();

        foreach (var path in files)
        {
            var baseOffset = long.Parse(
                Path.GetFileNameWithoutExtension(path),
                CultureInfo.InvariantCulture);
            var segment = new Segment(baseOffset, path);

            var scan = ScanSegment(path) ?? (0L, default, 0L);
            segment.RecordCount = scan.RecordCount;
            segment.NewestRecordUtc = scan.NewestUtc;

            // Crash during append can leave a torn tail; truncate to the last
            // valid framed record so the segment is appendable again.
            if (scan.ValidBytes < new FileInfo(path).Length)
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
                stream.SetLength(scan.ValidBytes);
            }

            segment.SizeBytes = scan.ValidBytes;
            _segments.Add(segment);
            _nextOffset = segment.BaseOffset + segment.RecordCount;
        }
    }

    /// <summary>
    /// Serializes one envelope, embedding the already-serialized payload bytes
    /// verbatim instead of re-parsing and re-serializing them.
    /// </summary>
    private static byte[] SerializeEnvelope(long offset, in PendingRecord record)
    {
        var buffer = new ArrayBufferWriter<byte>(record.PayloadJson.Length + 128);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("offset", offset);
            writer.WriteString("eventId", record.EventId);
            writer.WriteString("publishedUtc", record.PublishedUtc);
            writer.WritePropertyName("headers");
            writer.WriteStartObject();
            if (record.Headers is { } headers)
            {
                foreach (var (key, value) in headers)
                {
                    writer.WriteString(key, value);
                }
            }

            writer.WriteEndObject();
            writer.WritePropertyName("payload");
            writer.WriteRawValue(record.PayloadJson, skipInputValidation: true);
            writer.WriteEndObject();
        }

        return buffer.WrittenSpan.ToArray();
    }

    private static byte[] FrameLine(ReadOnlySpan<byte> json)
    {
        // Frame: 8 hex CRC chars, space, JSON, newline.
        var crc = Crc32.Compute(json);
        var line = new byte[8 + 1 + json.Length + 1];
        var crcText = crc.ToString("x8", CultureInfo.InvariantCulture);
        for (var i = 0; i < 8; i++)
        {
            line[i] = (byte)crcText[i];
        }

        line[8] = (byte)' ';
        json.CopyTo(line.AsSpan(9));
        line[^1] = (byte)'\n';
        return line;
    }

    /// <summary>
    /// Streams a segment from <paramref name="baseOffset"/>, yielding records in
    /// <c>[fromOffset, endOffsetExclusive)</c>. Records before
    /// <paramref name="fromOffset"/> are skipped by line position without being
    /// CRC-checked or deserialized, so a caught-up reader pays only for new data.
    /// </summary>
    private static IEnumerable<RecordEnvelope> ReadSegmentFrom(
        string path,
        long baseOffset,
        long fromOffset,
        long endOffsetExclusive)
    {
        var stream = TryOpenRead(path);
        if (stream is null)
        {
            // Deleted by retention between snapshot and read; its records are gone.
            yield break;
        }

        using (stream)
        {
            var reader = new FrameReader(stream);
            for (var offset = baseOffset; offset < endOffsetExclusive; offset++)
            {
                var skip = offset < fromOffset;
                var line = reader.ReadLine(skip);
                if (line is null)
                {
                    yield break;
                }

                if (skip)
                {
                    continue;
                }

                if (!TryParseFrame(line, out var envelope))
                {
                    yield break;
                }

                yield return envelope;
            }
        }
    }

    /// <summary>
    /// Streams a whole segment, counting valid framed records and reporting the
    /// byte offset just past the last one. Returns null if the file is gone.
    /// </summary>
    private static (long RecordCount, DateTimeOffset NewestUtc, long ValidBytes)? ScanSegment(string path)
    {
        var stream = TryOpenRead(path);
        if (stream is null)
        {
            return null;
        }

        using (stream)
        {
            var reader = new FrameReader(stream);
            var count = 0L;
            var newest = default(DateTimeOffset);
            var validBytes = 0L;
            while (reader.ReadLine(skip: false) is { } line)
            {
                if (!TryParseFrame(line, out var envelope))
                {
                    break;
                }

                count++;
                newest = envelope.PublishedUtc;
                validBytes = reader.LineEnd;
            }

            return (count, newest, validBytes);
        }
    }

    private static FileStream? TryOpenRead(string path)
    {
        try
        {
            return new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                ReadBufferBytes,
                FileOptions.SequentialScan);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    private static bool TryParseFrame(ReadOnlySpan<byte> line, out RecordEnvelope envelope)
    {
        envelope = null!;

        // Frame: 8 hex chars, space, at least one JSON byte.
        if (line.Length < 10 || line[8] != (byte)' ')
        {
            return false;
        }

        Span<char> hex = stackalloc char[8];
        for (var i = 0; i < 8; i++)
        {
            hex[i] = (char)line[i];
        }

        if (!uint.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var expectedCrc))
        {
            return false;
        }

        var json = line[9..];
        if (Crc32.Compute(json) != expectedCrc)
        {
            return false;
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<RecordEnvelope>(json);
            if (parsed is null)
            {
                return false;
            }

            envelope = parsed;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// Reads newline-framed lines from a stream one at a time with a bounded
    /// buffer, so a segment is never loaded whole. Skipped lines are consumed
    /// without materializing their bytes.
    /// </summary>
    private sealed class FrameReader(Stream stream)
    {
        private readonly byte[] _buffer = new byte[ReadBufferBytes];
        private int _start;
        private int _end;
        private long _bytesRead;
        private bool _eof;

        /// <summary>Absolute byte offset just past the last line returned by <see cref="ReadLine"/>.</summary>
        public long LineEnd { get; private set; }

        /// <summary>
        /// Consumes the next newline-terminated line, returning its bytes without
        /// the newline (or an empty array when <paramref name="skip"/> is true).
        /// Returns null at end of file or on a torn, unterminated tail.
        /// </summary>
        public byte[]? ReadLine(bool skip)
        {
            List<byte>? spillover = null;
            while (true)
            {
                if (_start < _end)
                {
                    var newline = Array.IndexOf(_buffer, (byte)'\n', _start, _end - _start);
                    if (newline >= 0)
                    {
                        byte[] line;
                        if (skip)
                        {
                            line = [];
                        }
                        else if (spillover is null)
                        {
                            line = _buffer[_start..newline];
                        }
                        else
                        {
                            spillover.AddRange(_buffer.AsSpan(_start, newline - _start));
                            line = [.. spillover];
                        }

                        _start = newline + 1;
                        LineEnd = _bytesRead - (_end - _start);
                        return line;
                    }

                    // No newline in the buffered region: keep the tail (unless
                    // skipping) and pull in the next chunk.
                    if (!skip && _end > _start)
                    {
                        (spillover ??= []).AddRange(_buffer.AsSpan(_start, _end - _start));
                    }

                    _start = _end;
                }

                if (!Refill())
                {
                    // End of file with no terminating newline: torn tail.
                    return null;
                }
            }
        }

        private bool Refill()
        {
            if (_eof)
            {
                return false;
            }

            _start = 0;
            _end = stream.Read(_buffer, 0, _buffer.Length);
            _bytesRead += _end;
            if (_end == 0)
            {
                _eof = true;
                return false;
            }

            return true;
        }
    }
}
