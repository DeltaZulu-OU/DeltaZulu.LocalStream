using System.Reflection;
using DeltaZulu.DurableBuffer;

namespace DeltaZulu.LocalStream.Tests;

/// <summary>
/// Regression guards for the DeltaZulu.DurableBuffer submodule dependency.
/// The gitlink for external/DeltaZulu.DurableBuffer was once lost in a rebase
/// (leaving .gitmodules without a pinned commit), which broke every fresh
/// checkout. These tests make the dependency's presence and integrity part of
/// the tested contract instead of a restore-time surprise.
/// </summary>
[TestClass]
public sealed class DurableBufferDependencyTests
{
    [TestMethod]
    public void DurableBufferAssembly_IsReferencedAndLoadable()
    {
        var assembly = typeof(JsonRecordSerializer<>).Assembly;

        Assert.AreEqual("DeltaZulu.DurableBuffer", assembly.GetName().Name);
    }

    [TestMethod]
    public void DurableBufferSerializer_IsUsableFromLocalStreamTypes()
    {
        // Exercise real submodule code, not just a type load: serialize a
        // LocalStream position with DurableBuffer's serializer.
        var serializer = new JsonRecordSerializer<StreamPosition>();

        var bytes = serializer.Serialize(new StreamPosition("agent.output", 0, 42));

        Assert.IsTrue(bytes.Length > 0);
        StringAssert.Contains(System.Text.Encoding.UTF8.GetString(bytes.Span), "agent.output");
    }

    [TestMethod]
    public void GitmodulesPaths_HaveGitlinksInTheRepositoryTree()
    {
        var repoRoot = FindRepositoryRoot();
        var gitmodulesPath = Path.Combine(repoRoot, ".gitmodules");
        Assert.IsTrue(File.Exists(gitmodulesPath), ".gitmodules must exist at the repository root");

        foreach (var submodulePath in ReadSubmodulePaths(gitmodulesPath))
        {
            var absolute = Path.Combine(repoRoot, submodulePath);
            Assert.IsTrue(
                Directory.Exists(absolute),
                $"Submodule directory '{submodulePath}' is missing. " +
                "Run: git submodule update --init --recursive");

            // An initialized submodule working tree contains a .git file (or
            // directory) and actual content. An empty directory means the
            // gitlink was lost or the submodule was never initialized.
            Assert.IsTrue(
                File.Exists(Path.Combine(absolute, ".git")) || Directory.Exists(Path.Combine(absolute, ".git")),
                $"Submodule '{submodulePath}' is not initialized (no .git entry). " +
                "If this happens in CI, the gitlink tree entry was probably lost in a rebase or merge: " +
                "re-add it with 'git add <path>' from a clone that has the submodule checked out.");

            Assert.IsTrue(
                Directory.EnumerateFileSystemEntries(absolute).Count() > 1,
                $"Submodule '{submodulePath}' is an empty checkout.");
        }
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, ".gitmodules")))
            {
                return directory.FullName;
            }

            directory = directory.Parent!;
        }

        throw new AssertFailedException("Could not locate the repository root from " + AppContext.BaseDirectory);
    }

    private static IEnumerable<string> ReadSubmodulePaths(string gitmodulesPath)
    {
        foreach (var line in File.ReadAllLines(gitmodulesPath))
        {
            var trimmed = line.Trim();
            if (trimmed.StartsWith("path", StringComparison.Ordinal))
            {
                yield return trimmed[(trimmed.IndexOf('=') + 1)..].Trim();
            }
        }
    }
}
