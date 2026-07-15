using System.Globalization;
using System.Text;
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

    public long Append(string eventId, DateTimeOffset publishedUtc, IReadOnlyDictionary<string, string>? headers, JsonElement payload)
    {
        lock (_sync)
        {
            var envelope = new RecordEnvelope
            {
                Offset = _nextOffset,
                EventId = eventId,
                PublishedUtc = publishedUtc,
                Headers = headers is null ? [] : new Dictionary<string, string>(headers),
                Payload = payload,
            };

            var json = JsonSerializer.SerializeToUtf8Bytes(envelope);
            var line = FrameLine(json);

            var segment = ActiveSegmentForWrite();
            using (var stream = new FileStream(segment.Path, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                stream.Write(line);
                stream.Flush(flushToDisk: true);
            }

            segment.SizeBytes += line.Length;
            segment.RecordCount++;
            segment.NewestRecordUtc = publishedUtc;
            return _nextOffset++;
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

            foreach (var envelope in ReadSegment(segment.Path))
            {
                if (envelope.Offset < fromOffset || envelope.Offset >= endOffsetExclusive)
                {
                    continue;
                }

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
            while (_segments.Count > 1 && ViolatesPolicy(retention, nowUtc))
            {
                var oldest = _segments[0];
                File.Delete(oldest.Path);
                _segments.RemoveAt(0);
            }
        }
    }

    private bool ViolatesPolicy(RetentionOptions retention, DateTimeOffset nowUtc)
    {
        var oldest = _segments[0];

        if (retention.MaxBytes is { } maxBytes && _segments.Sum(s => s.SizeBytes) > maxBytes)
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

    private Segment ActiveSegmentForWrite()
    {
        var active = _segments.Count > 0 ? _segments[^1] : null;
        if (active is null || active.SizeBytes >= _maxSegmentBytes)
        {
            active = new Segment(_nextOffset, SegmentPath(_nextOffset));
            using (File.Create(active.Path))
            {
            }

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

            var (envelopes, validBytes) = ScanSegment(path);
            foreach (var envelope in envelopes)
            {
                segment.RecordCount++;
                segment.NewestRecordUtc = envelope.PublishedUtc;
            }

            // Crash during append can leave a torn tail; truncate to the last
            // valid framed record so the segment is appendable again.
            if (validBytes < new FileInfo(path).Length)
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
                stream.SetLength(validBytes);
            }

            segment.SizeBytes = validBytes;
            _segments.Add(segment);
            _nextOffset = segment.BaseOffset + segment.RecordCount;
        }
    }

    private static byte[] FrameLine(byte[] json)
    {
        var crc = Crc32.Compute(json);
        var prefix = Encoding.ASCII.GetBytes(crc.ToString("x8", CultureInfo.InvariantCulture) + " ");
        var line = new byte[prefix.Length + json.Length + 1];
        prefix.CopyTo(line, 0);
        json.CopyTo(line, prefix.Length);
        line[^1] = (byte)'\n';
        return line;
    }

    private static IEnumerable<RecordEnvelope> ReadSegment(string path)
    {
        byte[] bytes;
        try
        {
            bytes = File.ReadAllBytes(path);
        }
        catch (FileNotFoundException)
        {
            // Deleted by retention between snapshot and read; its records are gone.
            yield break;
        }

        foreach (var (envelope, _) in ParseLines(bytes))
        {
            yield return envelope;
        }
    }

    private static (IReadOnlyList<RecordEnvelope> Envelopes, long ValidBytes) ScanSegment(string path)
    {
        var bytes = File.ReadAllBytes(path);
        var envelopes = new List<RecordEnvelope>();
        var end = 0L;
        foreach (var (envelope, lineEnd) in ParseLines(bytes))
        {
            envelopes.Add(envelope);
            end = lineEnd;
        }

        return (envelopes, end);
    }

    /// <summary>
    /// Parses framed lines, stopping at the first torn or corrupt line. Yields
    /// each envelope with the byte position just past its trailing newline.
    /// </summary>
    private static IEnumerable<(RecordEnvelope Envelope, long LineEnd)> ParseLines(byte[] bytes)
    {
        var position = 0;
        while (position < bytes.Length)
        {
            var newline = Array.IndexOf(bytes, (byte)'\n', position);
            if (newline < 0)
            {
                yield break;
            }

            // Frame: 8 hex chars, space, JSON.
            const int JsonStartRelative = 9;
            var jsonStart = position + JsonStartRelative;
            if (newline <= jsonStart)
            {
                yield break;
            }

            var crcText = Encoding.ASCII.GetString(bytes, position, 8);
            if (!uint.TryParse(crcText, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var expectedCrc)
                || bytes[position + 8] != (byte)' ')
            {
                yield break;
            }

            var json = bytes.AsSpan(jsonStart, newline - jsonStart);
            if (Crc32.Compute(json) != expectedCrc)
            {
                yield break;
            }

            RecordEnvelope? envelope;
            try
            {
                envelope = JsonSerializer.Deserialize<RecordEnvelope>(json);
            }
            catch (JsonException)
            {
                yield break;
            }

            if (envelope is null)
            {
                yield break;
            }

            position = newline + 1;
            yield return (envelope, position);
        }
    }
}
