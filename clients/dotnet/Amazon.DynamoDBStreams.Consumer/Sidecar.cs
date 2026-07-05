using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// Locates the <c>amazon-dynamodb-streams-consumer-sidecar</c> binary. NuGet
/// ships managed assemblies, not a native binary, so — like the Go and Node
/// clients — the sidecar is downloaded once from the GitHub Release
/// (checksum-verified) and cached. Resolution order: explicit path → env
/// override → cached download → download → PATH.
/// </summary>
internal static class Sidecar
{
    private const string Binary = "amazon-dynamodb-streams-consumer-sidecar";
    public const string Version = "0.1.3";
    private const string DefaultReleaseBase =
        "https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/download";

    private static string ReleaseBase() =>
        Environment.GetEnvironmentVariable("DDB_STREAMS_CONSUMER_RELEASE_BASE") ?? DefaultReleaseBase;

    public static (string Os, string Arch, string Ext) PlatformArch()
    {
        var os = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "windows"
            : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "darwin"
            : "linux";
        var arch = RuntimeInformation.OSArchitecture switch
        {
            Architecture.X64 => "x86_64",
            Architecture.Arm64 => "aarch64",
            var a => a.ToString().ToLowerInvariant(),
        };
        var ext = os == "windows" ? ".exe" : "";
        return (os, arch, ext);
    }

    public static string CachePath()
    {
        var (_, _, ext) = PlatformArch();
        var baseDir = Environment.GetEnvironmentVariable("XDG_CACHE_HOME")
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".cache");
        return Path.Combine(baseDir, "amazon-dynamodb-streams-consumer", Version, Binary + ext);
    }

    public static async Task<string> DiscoverAsync(string? explicitPath)
    {
        if (!string.IsNullOrEmpty(explicitPath))
        {
            return explicitPath!;
        }
        var env = Environment.GetEnvironmentVariable("DDB_STREAMS_CONSUMER_SIDECAR");
        if (!string.IsNullOrEmpty(env))
        {
            return env!;
        }
        var cached = CachePath();
        if (File.Exists(cached))
        {
            return cached;
        }
        try
        {
            return await DownloadAsync(cached).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            var onPath = OnPath();
            if (onPath != null)
            {
                return onPath;
            }
            throw new InvalidOperationException(
                $"could not obtain the {Binary} sidecar: download failed ({e.Message}) and it is not on " +
                "PATH. Set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar or install it manually.", e);
        }
    }

    private static async Task<string> DownloadAsync(string dst)
    {
        var (os, arch, ext) = PlatformArch();
        var asset = $"{Binary}-{os}-{arch}{ext}";
        var baseUrl = ReleaseBase().TrimEnd('/');
        var binUrl = $"{baseUrl}/v{Version}/{asset}";

        using var http = new HttpClient();
        var wantLine = (await http.GetStringAsync(binUrl + ".sha256").ConfigureAwait(false)).Trim();
        var want = wantLine.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries)[0];
        var body = await http.GetByteArrayAsync(binUrl).ConfigureAwait(false);
        var got = Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant();
        if (got != want.ToLowerInvariant())
        {
            throw new InvalidOperationException($"checksum mismatch for {asset}: got {got} want {want}");
        }

        Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
        var tmp = $"{dst}.tmp-{Environment.ProcessId}";
        await File.WriteAllBytesAsync(tmp, body).ConfigureAwait(false);
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            File.SetUnixFileMode(
                tmp,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
                    | UnixFileMode.GroupRead | UnixFileMode.GroupExecute
                    | UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
        }
        File.Move(tmp, dst, overwrite: true);
        return dst;
    }

    private static string? OnPath()
    {
        var (_, _, ext) = PlatformArch();
        var name = Binary + ext;
        var path = Environment.GetEnvironmentVariable("PATH") ?? "";
        foreach (var dir in path.Split(Path.PathSeparator))
        {
            if (string.IsNullOrEmpty(dir))
            {
                continue;
            }
            var p = Path.Combine(dir, name);
            if (File.Exists(p))
            {
                return p;
            }
        }
        return null;
    }
}
