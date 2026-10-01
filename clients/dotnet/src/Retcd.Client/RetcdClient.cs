using System.Runtime.CompilerServices;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Pb = Retcd.V1;

namespace Retcd.Client;

/// <summary>
/// A client for a rEtcd cluster. Create one, share it, dispose it.
/// It follows the leader by itself. It retries only when nothing was applied
/// (not the leader, cannot connect). A write that times out is never retried:
/// you get <see cref="UnknownOutcomeException"/>.
/// </summary>
public sealed partial class RetcdClient : IAsyncDisposable, IDisposable
{
    private const string NowProbeKey = ".retcd-client/none";

    private readonly RetcdClientOptions _options;
    private readonly Transport _transport;
    private readonly HttpClient _http;

    private RetcdClient(RetcdClientOptions options)
    {
        _options = options;
        _transport = new Transport(options.Endpoints);
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
    }

    /// <summary>Make a client. No connection is opened until the first call.</summary>
    public static RetcdClient Create(RetcdClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options), "Timeout must be positive");
        return new RetcdClient(options);
    }

    /// <summary>Test hook: drop the connection to the current node, as a network break would.</summary>
    internal void DropCurrentConnection() => _transport.Drop(_transport.Current);

    /// <summary>The node the client is using right now ("http://host:port").</summary>
    public string CurrentEndpoint => _transport.Current;

    public void Dispose()
    {
        _transport.Dispose();
        _http.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ------------------------------------------------------------------------------------
    // Get / Put / Delete
    // ------------------------------------------------------------------------------------

    /// <summary>Read one key from the leader. Null when it does not exist.</summary>
    public async Task<RetcdRecord?> GetAsync(string key, CancellationToken ct = default)
    {
        CheckKey(key);
        var r = await UnaryAsync("get", isWrite: false, 0,
            (c, o) => c.GetAsync(new Pb.GetRequest { Key = Bytes(key) }, o), ct).ConfigureAwait(false);
        return r.Record is null ? null : ToRecord(r.Record);
    }

    /// <summary>
    /// Write a value. Returns the new revision. With <paramref name="ifRevision"/> it only writes if the key
    /// is still at that revision (0 = only if the key does not exist) and throws <see cref="CasConflictException"/> otherwise.
    /// </summary>
    public Task<ulong> PutAsync(string key, string value, ulong? ifRevision = null, CancellationToken ct = default)
        => PutAsync(key, new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value)), ifRevision, ct);

    /// <inheritdoc cref="PutAsync(string, string, ulong?, CancellationToken)"/>
    public async Task<ulong> PutAsync(string key, ReadOnlyMemory<byte> value, ulong? ifRevision = null, CancellationToken ct = default)
    {
        CheckKey(key);
        if (value.Length > Limits.MaxValueBytes) throw new ValueTooLargeException("value", value.Length, Limits.MaxValueBytes);
        var req = new Pb.PutRequest { Key = Bytes(key), Value = UnsafeByteOperations.UnsafeWrap(value) };
        if (ifRevision is { } rev) req.ExpectedModRevision = rev;
        var r = await UnaryAsync("put", isWrite: true, value.Length,
            (c, o) => c.PutAsync(req, o), ct).ConfigureAwait(false);
        return r.Outcome switch
        {
            Pb.MutationOutcome.Applied => r.Revision,
            Pb.MutationOutcome.Conflict => throw new CasConflictException(r.CurrentModRevision, r.Exists),
            _ => throw new RetcdException($"put: unexpected outcome {r.Outcome}"),
        };
    }

    /// <summary>
    /// Delete a key. True if it was deleted, false if it did not exist.
    /// With <paramref name="ifRevision"/> it only deletes if the key is at that revision, else throws <see cref="CasConflictException"/>.
    /// </summary>
    public async Task<bool> DeleteAsync(string key, ulong? ifRevision = null, CancellationToken ct = default)
    {
        CheckKey(key);
        var req = new Pb.DeleteRequest { Key = Bytes(key) };
        if (ifRevision is { } rev) req.ExpectedModRevision = rev;
        var r = await UnaryAsync("delete", isWrite: true, 0,
            (c, o) => c.DeleteAsync(req, o), ct).ConfigureAwait(false);
        return r.Outcome switch
        {
            Pb.MutationOutcome.Applied => true,
            Pb.MutationOutcome.NotFound => false,
            Pb.MutationOutcome.Conflict => throw new CasConflictException(r.CurrentModRevision, r.Exists),
            _ => throw new RetcdException($"delete: unexpected outcome {r.Outcome}"),
        };
    }

    // ------------------------------------------------------------------------------------
    // List
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// List keys. A plain prefix ("docs/") lists everything under it. A pattern ("docs/*", "docs/**/*.md")
    /// is matched here, after the server lists the literal prefix. Pages are fetched as you enumerate.
    /// All pages come from one pinned revision. If the cursor is refused (leader change, expiry) it throws:
    /// start the list again.
    /// </summary>
    public async IAsyncEnumerable<RetcdRecord> ListAsync(
        string patternOrPrefix, int pageSize = 500, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var prefix = RetcdGlob.LiteralPrefix(patternOrPrefix);
        var re = RetcdGlob.HasGlob(patternOrPrefix) ? RetcdGlob.ToRegex(patternOrPrefix) : null;
        await foreach (var page in PagesAsync(prefix, pageSize, ct).ConfigureAwait(false))
        {
            foreach (var rec in page.Records)
            {
                var r = ToRecord(rec);
                if (re is null || re.IsMatch(r.Key)) yield return r;
            }
        }
    }

    /// <summary>
    /// Like <see cref="ListAsync"/>, but also returns one <see cref="RetcdDir"/> per matching sub-folder
    /// (only when the pattern has no "**"). A plain prefix is treated as a folder: "docs" means "docs/*".
    /// </summary>
    public async Task<RetcdDirListing> ListDirsAsync(string pattern, int pageSize = 500, CancellationToken ct = default)
    {
        if (!RetcdGlob.HasGlob(pattern)) pattern = (pattern.EndsWith('/') ? pattern : pattern + "/") + "*";
        var re = RetcdGlob.ToRegex(pattern);
        var wantDirs = !pattern.Contains("**", StringComparison.Ordinal);
        var files = new List<RetcdRecord>();
        var dirs = new Dictionary<string, int>();
        var dirOrder = new List<string>();
        await foreach (var page in PagesAsync(RetcdGlob.LiteralPrefix(pattern), pageSize, ct).ConfigureAwait(false))
        {
            foreach (var rec in page.Records)
            {
                var r = ToRecord(rec);
                if (re.IsMatch(r.Key)) files.Add(r);
                if (!wantDirs) continue;
                for (var i = r.Key.IndexOf('/'); i != -1; i = r.Key.IndexOf('/', i + 1))
                {
                    var dir = r.Key[..i];
                    if (!re.IsMatch(dir)) continue;
                    var path = dir + "/";
                    if (dirs.TryGetValue(path, out var n)) dirs[path] = n + 1;
                    else
                    {
                        dirs[path] = 1;
                        dirOrder.Add(path);
                    }
                }
            }
        }
        return new RetcdDirListing(files, dirOrder.Select(p => new RetcdDir(p, dirs[p])).ToList());
    }

    /// <summary>Every record under a prefix plus the revision the list was read at. Used by presence.</summary>
    internal async Task<(List<RetcdRecord> Records, ulong ReadRevision)> ListAllWithRevisionAsync(string prefix, CancellationToken ct)
    {
        var all = new List<RetcdRecord>();
        ulong readRev = 0;
        await foreach (var page in PagesAsync(prefix, 500, ct).ConfigureAwait(false))
        {
            readRev = page.ReadRevision; // every page of a pinned walk reports the same revision
            foreach (var rec in page.Records) all.Add(ToRecord(rec));
        }
        return (all, readRev);
    }

    private async IAsyncEnumerable<Pb.ListResponse> PagesAsync(
        string prefix, int pageSize, [EnumeratorCancellation] CancellationToken ct)
    {
        if (pageSize < 1 || pageSize > Limits.MaxListItems)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be 1..{Limits.MaxListItems}");
        }
        if (Encoding.UTF8.GetByteCount(prefix) > Limits.MaxKeyBytes) throw new ArgumentException("prefix is longer than a key can be", nameof(prefix));
        var token = ByteString.Empty; // present but empty: start a pinned walk
        for (;;)
        {
            var req = new Pb.ListRequest { Prefix = Bytes(prefix), MaxItems = (uint)pageSize, PageToken = token };
            var resp = await UnaryAsync("list", isWrite: false, 0, (c, o) => c.ListAsync(req, o), ct,
                followForeignPageToken: true).ConfigureAwait(false);
            yield return resp;
            if (resp.HasNextPageToken && resp.NextPageToken.Length > 0)
            {
                token = resp.NextPageToken;
                continue;
            }
            if (resp.Truncated)
            {
                throw new RetcdException("list: the server cut the list short at a size cap and gave no cursor. Use a longer prefix or a smaller pageSize.");
            }
            yield break;
        }
    }

    // ------------------------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Ask every node's health endpoint, one row per node. A node that does not answer is a row with
    /// Reachable = false, not an exception. Health ports default to client port plus 2.
    /// </summary>
    public async Task<IReadOnlyList<NodeHealth>> HealthAsync(CancellationToken ct = default)
    {
        var endpoints = _options.Endpoints.Select(_transport.Normalize).ToList();
        var configured = _options.HealthEndpoints;
        if (configured is not null && configured.Count != _options.Endpoints.Count)
        {
            throw new ArgumentException("HealthEndpoints must have one entry per entry of Endpoints");
        }
        var tasks = new List<Task<NodeHealth>>();
        for (var i = 0; i < endpoints.Count; i++)
        {
            var ep = endpoints[i];
            var url = configured is null ? DeriveHealthUrl(ep) : $"http://{configured[i]}/health";
            tasks.Add(ReadHealthAsync(ep, url, ct));
        }
        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    internal static string DeriveHealthUrl(string clientEndpoint)
    {
        var u = new Uri(clientEndpoint);
        return $"http://{u.Host}:{u.Port + 2}/health";
    }

    private async Task<NodeHealth> ReadHealthAsync(string endpoint, string url, CancellationToken ct)
    {
        try
        {
            using var resp = await _http.GetAsync(url, ct).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            var text = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            using var doc = System.Text.Json.JsonDocument.Parse(text);
            var root = doc.RootElement;
            return new NodeHealth(endpoint, url, true, null,
                Ready: root.TryGetProperty("ready", out var ready) && ready.ValueKind == System.Text.Json.JsonValueKind.True,
                Role: Str(root, "role"),
                NodeId: UInt(root, "node_id"),
                CurrentLeader: UInt(root, "current_leader"),
                ClusterRevision: UInt(root, "cluster_revision"),
                StateHashHex: Str(root, "state_hash_hex"));
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && ex is HttpRequestException or TaskCanceledException or System.Text.Json.JsonException)
        {
            return new NodeHealth(endpoint, url, false, ex.Message, false, null, null, null, null, null);
        }

        static string? Str(System.Text.Json.JsonElement e, string name) =>
            e.TryGetProperty(name, out var v) && v.ValueKind == System.Text.Json.JsonValueKind.String ? v.GetString() : null;
        static ulong? UInt(System.Text.Json.JsonElement e, string name) =>
            e.TryGetProperty(name, out var v) && v.ValueKind == System.Text.Json.JsonValueKind.Number && v.TryGetUInt64(out var n) ? n : null;
    }

    // ------------------------------------------------------------------------------------
    // Plumbing
    // ------------------------------------------------------------------------------------

    private static ByteString Bytes(string s) => ByteString.CopyFromUtf8(s);

    private static RetcdRecord ToRecord(Pb.Record r) =>
        new(r.Key.ToStringUtf8(), r.Value.Memory, r.CreateRevision, r.ModRevision);

    private static void CheckKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length == 0) throw new ArgumentException("key is empty", nameof(key));
        var n = Encoding.UTF8.GetByteCount(key);
        if (n > Limits.MaxKeyBytes) throw new ArgumentException($"key is {n} bytes; the server limit is {Limits.MaxKeyBytes}", nameof(key));
    }

    private static Task Pause(int ms, CancellationToken ct) => Task.Delay(ms, ct);

    /// <summary>
    /// One logical call. Follows "not the leader" (nothing was applied) and moves on from a node that
    /// refuses the connection (nothing was sent). Everything else is mapped and thrown. A write is never re-sent after it may have reached a node.
    /// </summary>
    private async Task<T> UnaryAsync<T>(
        string op, bool isWrite, long valueSize,
        Func<Pb.ConfigService.ConfigServiceClient, CallOptions, AsyncUnaryCall<T>> send,
        CancellationToken ct, bool followForeignPageToken = false)
    {
        var deadline = DateTime.UtcNow + _options.Timeout;
        string? lastNote = null;
        var hops = 0;
        for (;;)
        {
            ct.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline)
            {
                throw new RetcdUnavailableException(
                    $"{op}: no node could take the call within {_options.Timeout.TotalSeconds:0.#} s ({lastNote ?? "no attempt made"}). Nothing was applied.");
            }
            var endpoint = _transport.Current;
            var connectFailure = await _transport.ConnectAsync(endpoint, deadline, ct).ConfigureAwait(false);
            if (connectFailure is not null)
            {
                lastNote = $"cannot connect to {endpoint} ({connectFailure.GetType().Name})";
                _transport.Drop(endpoint);
                _transport.Rotate();
                await Pause(100, ct).ConfigureAwait(false);
                continue;
            }
            try
            {
                using var call = send(_transport.ClientFor(endpoint), new CallOptions(deadline: deadline, cancellationToken: ct));
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
                if (RetcdErrors.IsNotLeader(ex) || (followForeignPageToken && RetcdErrors.IsForeignPageToken(ex)))
                {
                    var hint = RetcdErrors.LeaderHint(ex);
                    lastNote = hint is null ? "no leader known yet" : $"not the leader; leader is {hint}";
                    if (hint is not null && _transport.Normalize(hint) != endpoint)
                    {
                        _transport.Use(hint);
                        if (++hops > 2) await Pause(200, ct).ConfigureAwait(false);
                    }
                    else
                    {
                        await Pause(200, ct).ConfigureAwait(false); // election in progress
                        if (hint is null) _transport.Rotate();
                    }
                    continue;
                }
                if (RetcdErrors.IsConnectFailure(ex))
                {
                    lastNote = $"cannot connect to {endpoint}";
                    _transport.Drop(endpoint);
                    _transport.Rotate();
                    await Pause(100, ct).ConfigureAwait(false);
                    continue;
                }
                throw RetcdErrors.Map(ex, op, isWrite, valueSize);
            }
        }
    }
}
