namespace DeltaZulu.LocalStream.Storage;

/// <summary>
/// File creation helpers applying the safety baseline: owner-only Unix file
/// modes and atomic replace via temp-file-plus-rename.
/// </summary>
internal static class SafeFiles
{
    private const UnixFileMode OwnerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    public static void CreateEmpty(string path)
    {
        using (new FileStream(path, CreateOptions()))
        {
        }
    }

    public static void WriteAllTextAtomic(string path, string contents)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, CreateOptions()))
        using (var writer = new StreamWriter(stream))
        {
            writer.Write(contents);
        }

        File.Move(temp, path, overwrite: true);
    }

    private static FileStreamOptions CreateOptions()
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.Create,
            Access = FileAccess.Write,
        };

        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = OwnerOnly;
        }

        return options;
    }
}
