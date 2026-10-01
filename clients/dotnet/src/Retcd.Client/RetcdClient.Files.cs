using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Retcd.Client;

public sealed partial class RetcdClient
{
    /// <summary>Where the meta record for a file key lives.</summary>
    public static string MetaKeyFor(string key) => "meta/" + key;

    /// <summary>
    /// Store a file (max 1 MiB) under <paramref name="key"/>, then a small JSON meta record at
    /// <c>meta/&lt;key&gt;</c>: <c>{"size","sha256","stored_at_rev"}</c> (same as kv.mjs putfile).
    /// Two writes, not atomic. If the second fails the file is stored without a meta record.
    /// </summary>
    public async Task<PutFileResult> PutFileAsync(string path, string key, CancellationToken ct = default)
    {
        CheckKey(key);
        CheckKey(MetaKeyFor(key));
        var info = new FileInfo(path);
        if (!info.Exists) throw new FileNotFoundException($"cannot read {path}", path);
        if (info.Length > Limits.MaxValueBytes) throw new ValueTooLargeException(path, info.Length, Limits.MaxValueBytes);

        var data = await File.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        var sha = Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();
        var rev = await PutAsync(key, data, null, ct).ConfigureAwait(false);

        var meta = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["size"] = data.Length,
            ["sha256"] = sha,
            ["stored_at_rev"] = rev,
        });
        ulong metaRev;
        try
        {
            metaRev = await PutAsync(MetaKeyFor(key), meta, null, ct).ConfigureAwait(false);
        }
        catch (RetcdException ex)
        {
            throw new RetcdException($"file stored at revision {rev}, but its meta record failed: {ex.Message}", ex, ex.GrpcStatus);
        }
        return new PutFileResult(key, data.Length, sha, rev, metaRev);
    }

    /// <summary>
    /// Fetch a file and check it against <c>meta/&lt;key&gt;</c> (size and sha256).
    /// Throws <see cref="ChecksumMismatchException"/> and writes nothing if they differ.
    /// With no meta record the file is written and <see cref="GetFileResult.Verified"/> is false.
    /// </summary>
    public async Task<GetFileResult> GetFileAsync(string key, string path, bool overwrite = false, CancellationToken ct = default)
    {
        CheckKey(key);
        if (File.Exists(path) && !overwrite) throw new IOException($"{path} already exists. Pass overwrite: true to replace it.");

        var rec = await GetAsync(key, ct).ConfigureAwait(false) ?? throw new RetcdNotFoundException(key);
        var data = rec.Value;
        var sha = Convert.ToHexString(SHA256.HashData(data.Span)).ToLowerInvariant();

        var verified = false;
        var metaRec = await GetAsync(MetaKeyFor(key), ct).ConfigureAwait(false);
        if (metaRec is not null)
        {
            string? metaSha;
            long? metaSize;
            try
            {
                using var doc = JsonDocument.Parse(metaRec.Value);
                metaSha = doc.RootElement.TryGetProperty("sha256", out var s) ? s.GetString() : null;
                metaSize = doc.RootElement.TryGetProperty("size", out var z) && z.TryGetInt64(out var zz) ? zz : null;
            }
            catch (JsonException ex)
            {
                throw new ChecksumMismatchException($"meta record for {key} is not valid JSON ({ex.Message}). File not written.");
            }
            if (!string.Equals(metaSha, sha, StringComparison.OrdinalIgnoreCase) || metaSize != data.Length)
            {
                throw new ChecksumMismatchException(
                    $"CHECK FAILED: stored {data.Length} bytes / sha256 {sha}, meta says {metaSize} bytes / {metaSha}. File not written.");
            }
            verified = true;
        }

        var tmp = path + ".retcd-part";
        await File.WriteAllBytesAsync(tmp, data.ToArray(), ct).ConfigureAwait(false);
        File.Move(tmp, path, overwrite: true);
        return new GetFileResult(key, path, data.Length, sha, rec.ModRevision, verified);
    }
}
