using System.Runtime.InteropServices;

namespace DeltaZulu.LocalStream.Storage;

/// <summary>
/// File creation helpers applying the safety baseline: owner-only Unix file
/// modes and atomic replace via temp-file-plus-rename, with both the file
/// data and the containing directory entry fsynced before returning.
/// </summary>
internal static class SafeFiles
{
    private const UnixFileMode OwnerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    public static void CreateEmpty(string path)
    {
        using (var stream = new FileStream(path, CreateOptions()))
        {
            stream.Flush(flushToDisk: true);
        }

        FsyncParentDirectory(path);
    }

    public static void WriteAllTextAtomic(string path, string contents)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, CreateOptions()))
        using (var writer = new StreamWriter(stream))
        {
            writer.Write(contents);
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }

        File.Move(temp, path, overwrite: true);
        FsyncParentDirectory(path);
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

    /// <summary>
    /// Fsyncs the parent directory of <paramref name="path"/> so the file's
    /// directory entry (a create, or the rename in <see cref="WriteAllTextAtomic"/>)
    /// is durable, not merely visible. Without this, a crash between the data
    /// fsync above and the directory entry reaching disk can lose the entry
    /// on recovery even though the data itself was flushed. Best-effort: a
    /// no-op on Windows (NTFS journals the metadata operation itself and
    /// there is no POSIX directory-fsync equivalent to call), and it swallows
    /// failure to open or sync the directory rather than fail the write —
    /// this hardens durability, it does not gate it.
    /// </summary>
    private static void FsyncParentDirectory(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (string.IsNullOrEmpty(directory))
        {
            return;
        }

        try
        {
            var fd = Open(directory, ORdOnly, 0);
            if (fd < 0)
            {
                return;
            }

            try
            {
                _ = Fsync(fd);
            }
            finally
            {
                _ = Close(fd);
            }
        }
        catch (DllNotFoundException)
        {
            // Platform without a resolvable libc (e.g. exotic musl layouts).
            // Directory durability is best-effort; data is already fsynced.
        }
        catch (EntryPointNotFoundException)
        {
            // As above.
        }
    }

    private const int ORdOnly = 0;

    // Opening a directory O_RDONLY (no O_DIRECTORY) is valid POSIX for the
    // sole purpose of fsync-ing it; only read() on the fd would require
    // O_DIRECTORY, and this never reads. Avoids O_DIRECTORY's differing
    // numeric value across Linux/macOS.
    [DllImport("libc", EntryPoint = "open", SetLastError = true, BestFitMapping = false, ThrowOnUnmappableChar = true)]
    private static extern int Open([MarshalAs(UnmanagedType.LPUTF8Str)] string pathname, int flags, int mode);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int Fsync(int fd);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int Close(int fd);
}
