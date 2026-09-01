using System.Globalization;
using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class ResultIdentityBuilderTests
{
    private static readonly SourceRange Range = new("authentication", 2, 100, 149);
    private static readonly WindowInterval Window = new(
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch.AddMinutes(5));

    [TestMethod]
    public void Build_SameLogicalRetryProducesSameIdentity()
    {
        var first = ResultIdentityBuilder.Build(CreateIdentity());
        var reconstructed = ResultIdentityBuilder.Build(CreateIdentity());

        Assert.AreEqual(first, reconstructed);
        Assert.AreEqual(71, first.Value.Length);
        Assert.AreEqual(first, ResultChangeId.Parse(first.Value));
    }

    [TestMethod]
    public void Build_DifferentCorrectionRevisionAndCausalityProduceDifferentIdentities()
    {
        var baseline = ResultIdentityBuilder.Build(CreateIdentity());

        Assert.AreNotEqual(baseline, ResultIdentityBuilder.Build(CreateIdentity(logicalVersion: 2)));
        Assert.AreNotEqual(baseline, ResultIdentityBuilder.Build(CreateIdentity(revision: 2)));
        Assert.AreNotEqual(baseline, ResultIdentityBuilder.Build(CreateIdentity(
            causality: new SourceRange("authentication", 2, 150, 199))));
    }

    [TestMethod]
    public void Build_LengthDelimitsFieldsAndIncludesWindow()
    {
        var first = CreateIdentity(queryId: "ab", outputBindingId: "c");
        var ambiguousConcatenation = CreateIdentity(queryId: "a", outputBindingId: "bc");
        var withoutWindow = CreateIdentity(hasWindow: false);

        Assert.AreNotEqual(ResultIdentityBuilder.Build(first), ResultIdentityBuilder.Build(ambiguousConcatenation));
        Assert.AreNotEqual(ResultIdentityBuilder.Build(first), ResultIdentityBuilder.Build(withoutWindow));
    }

    [TestMethod]
    public void Build_IsIndependentOfCurrentCulture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            var turkish = ResultIdentityBuilder.Build(CreateIdentity());
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            Assert.AreEqual(turkish, ResultIdentityBuilder.Build(CreateIdentity()));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [TestMethod]
    public void Parse_RejectsMalformedOrUnknownIdentity()
    {
        Assert.ThrowsExactly<FormatException>(() => ResultChangeId.Parse("DZLSQ2_" + new string('A', 64)));
        Assert.ThrowsExactly<FormatException>(() => ResultChangeId.Parse("DZLSQ1_" + new string('z', 64)));
    }

    private static ResultIdentity CreateIdentity(
        string queryId = "query",
        long revision = 1,
        string outputBindingId = "detections",
        bool hasWindow = true,
        long logicalVersion = 1,
        SourceRange? causality = null) => new(
            queryId,
            revision,
            outputBindingId,
            "window-aggregate",
            "192.0.2.1",
            hasWindow ? Window : null,
            logicalVersion,
            causality ?? Range);
}
