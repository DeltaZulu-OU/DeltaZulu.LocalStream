namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Versioned deterministic identity of one observable result change.</summary>
public readonly record struct ResultChangeId
{
    private const string Prefix = "DZLSQ1_";
    private const int HashCharacters = 64;

    private ResultChangeId(string value) => Value = value;

    public string Value { get; }

    public static ResultChangeId Parse(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length != Prefix.Length + HashCharacters
            || !value.StartsWith(Prefix, StringComparison.Ordinal)
            || !IsUpperHex(value.AsSpan(Prefix.Length)))
        {
            throw new FormatException("Result change identity has an unsupported format.");
        }

        return new ResultChangeId(value);
    }

    internal static ResultChangeId FromHash(ReadOnlySpan<byte> hash) =>
        new(Prefix + Convert.ToHexString(hash));

    private static bool IsUpperHex(ReadOnlySpan<char> value)
    {
        foreach (var character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'A' and <= 'F'))
            {
                return false;
            }
        }

        return true;
    }

    public override string ToString() => Value;
}
