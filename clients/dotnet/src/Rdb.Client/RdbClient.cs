using System.Diagnostics;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Pb = Rdb.V1;

namespace Rdb.Client;

/// <summary>
/// A client for rDB through the rdb-server gateway. Create one, share it, dispose it.
/// <para>
/// It sends a call again only when nothing was applied: it could not connect (nothing was sent), or the server refused
/// the call and proved it changed nothing. A write whose outcome is not known is never sent again: you get
/// <see cref="RdbUnknownOutcomeException"/>, and <see cref="StatusAsync"/> settles it.
/// </para>
/// </summary>
public sealed class RdbClient : IDisposable, IAsyncDisposable
{
    /// <summary>How long one connect may take before the client counts it as failed (nothing sent).</summary>
    private static readonly TimeSpan ConnectWindow = TimeSpan.FromSeconds(3);

    /// <summary>Slack on top of the server's own worst case, for the hop and a route re-read.</summary>
    private static readonly TimeSpan Slack = TimeSpan.FromSeconds(2);

    /// <summary>The host's waiter margin on a transaction, twice (rdb_api TXN_WAITER_MARGIN_MILLIS, 2 s).</summary>
    private static readonly TimeSpan TxnMargins = TimeSpan.FromSeconds(4);

    private static readonly TimeSpan DefaultTxnDeadline = TimeSpan.FromSeconds(5);

    private readonly RdbClientOptions _options;
    private readonly string _endpoint;
    private readonly object _lock = new();
    private GrpcChannel? _channel;
    private bool _disposed;

    // The server's own waits. Defaults are rdb_api's; ReadyAsync replaces them with what the server reports.
    private TimeSpan _serverPut = TimeSpan.FromSeconds(5);
    private TimeSpan _serverRead = TimeSpan.FromSeconds(1);

    private RdbClient(RdbClientOptions options)
    {
        _options = options;
        _endpoint = Normalize(options.Endpoint);
    }

    /// <summary>Make a client. No connection is opened until the first call.</summary>
    public static RdbClient Create(RdbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options), "Timeout must be positive");
        return new RdbClient(options);
    }

    /// <summary>"host:port" becomes "http://host:port".</summary>
    public static string Normalize(string endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        var e = endpoint.Trim();
        if (!e.Contains("://", StringComparison.Ordinal)) e = "http://" + e;
        if (!Uri.TryCreate(e, UriKind.Absolute, out var uri) || uri.Port <= 0 || e.TrimEnd('/').EndsWith(":", StringComparison.Ordinal))
        {
            throw new ArgumentException($"endpoint \"{endpoint}\" is not host:port or http://host:port");
        }
        return $"{uri.Scheme}://{uri.Host}:{uri.Port}";
    }

    public string Endpoint => _endpoint;

    /// <summary>Test seam: when set, calls go through this invoker instead of a channel.</summary>
    internal CallInvoker? FakeInvoker { get; set; }

    /// <summary>Test seam: when set, the connect step asks this instead of opening a socket. Null = connected.</summary>
    internal Func<Exception?>? FakeConnect { get; set; }

    /// <summary>Test seam: the pause between attempts. Default Task.Delay.</summary>
    internal Func<TimeSpan, CancellationToken, Task> Pause { get; set; } = Task.Delay;

    // ------------------------------------------------------------------------------------
    // Calls
    // ------------------------------------------------------------------------------------

    /// <summary>Whether the database admits writes. The server waits up to <paramref name="within"/> (default 1 s, at most 10 s).</summary>
    public async Task<RdbReady> ReadyAsync(TimeSpan? within = null, CancellationToken ct = default)
    {
        var w = within ?? TimeSpan.FromSeconds(1);
        var req = new Pb.ReadyRequest { WithinMs = (uint)Math.Clamp(w.TotalMilliseconds, 1, 10_000) };
        var r = await CallAsync("ready", isWrite: false, w + Slack, (c, o) => c.ReadyAsync(req, o), ct).ConfigureAwait(false);
        var put = TimeSpan.FromMilliseconds(r.PutTimeoutMs);
        var read = TimeSpan.FromMilliseconds(r.ReadTimeoutMs);
        lock (_lock)
        {
            if (r.PutTimeoutMs > 0) _serverPut = put;
            if (r.ReadTimeoutMs > 0) _serverRead = read;
        }
        return new RdbReady(r.Ready, r.Detail, r.ClientId, put, read);
    }

    /// <summary>Replace a key's value (UTF-8), optionally only if it is at <paramref name="ifVersion"/>.</summary>
    public Task<RdbWrite> PutAsync(string key, string value, ulong? ifVersion = null, CancellationToken ct = default) =>
        PutAsync(Utf8(key, nameof(key)), Utf8(value, nameof(value)), ifVersion, ct);

    /// <summary>Replace a key's value, optionally only if it is at <paramref name="ifVersion"/>.</summary>
    public async Task<RdbWrite> PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, ulong? ifVersion = null, CancellationToken ct = default)
    {
        var req = new Pb.PutRequest { Object = ByteString.CopyFrom(key.Span), Value = ByteString.CopyFrom(value.Span) };
        if (ifVersion is { } v) req.IfVersion = v;
        var r = await CallAsync("put", isWrite: true, PutAttempt(), (c, o) => c.PutAsync(req, o), ct).ConfigureAwait(false);
        return Write(r);
    }

    /// <summary>Read a key. Null when it does not exist.</summary>
    public Task<RdbRecord?> GetAsync(string key, CancellationToken ct = default) => GetAsync(Utf8(key, nameof(key)), ct);

    /// <summary>Read a key. Null when it does not exist.</summary>
    public async Task<RdbRecord?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        var req = new Pb.GetRequest { Object = ByteString.CopyFrom(key.Span) };
        var r = await CallAsync("get", isWrite: false, ReadAttempt(), (c, o) => c.GetAsync(req, o), ct).ConfigureAwait(false);
        return r.Found ? new RdbRecord(r.Version, r.Value.Memory, r.Generation, r.RecoveryMode) : null;
    }

    /// <summary>Write every put, or none. <paramref name="deadline"/> is the server's deadline (default 5 s, at most 30 s).</summary>
    public async Task<RdbWrite> TxnAsync(IEnumerable<RdbTxnPut> puts, TimeSpan? deadline = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(puts);
        var d = deadline ?? DefaultTxnDeadline;
        if (d <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(deadline), "deadline must be positive");
        var req = new Pb.TxnRequest { DeadlineMs = (uint)Math.Min(d.TotalMilliseconds, uint.MaxValue) };
        foreach (var p in puts)
        {
            var tp = new Pb.TxnPut { Object = ByteString.CopyFrom(p.Key.Span), Value = ByteString.CopyFrom(p.Value.Span) };
            if (p.IfVersion is { } v) tp.IfVersion = v;
            req.Puts.Add(tp);
        }
        var r = await CallAsync("txn", isWrite: true, TxnAttempt(d), (c, o) => c.TxnAsync(req, o), ct).ConfigureAwait(false);
        return Write(r);
    }

    /// <summary>
    /// What became of a write. Pass the request id and generation from <see cref="RdbWrite"/> or from an
    /// <see cref="RdbException"/>. Without the generation, an answer the server already dropped says NotKnown.
    /// </summary>
    public async Task<RdbStatus> StatusAsync(ulong requestId, ulong? generation = null, CancellationToken ct = default)
    {
        var req = new Pb.StatusRequest { RequestId = requestId };
        if (generation is { } g) req.Generation = g;
        var r = await CallAsync("status", isWrite: false, ReadAttempt(), (c, o) => c.StatusAsync(req, o), ct).ConfigureAwait(false);
        return r.State switch
        {
            Pb.StatusState.Resolved => new RdbStatus(RdbLanded.Yes, r.Version, r.Generation, (RdbOutcome)(int)r.Outcome),
            Pb.StatusState.Expired => new RdbStatus(RdbLanded.ReadTheObject, 0, 0, RdbOutcome.Unspecified),
            _ => new RdbStatus(RdbLanded.NotKnown, 0, 0, RdbOutcome.Unspecified),
        };
    }

    // ------------------------------------------------------------------------------------
    // Attempts
    // ------------------------------------------------------------------------------------

    /// <summary>A put's worst case on the server: compile (put timeout) plus send (put timeout), plus slack.</summary>
    internal TimeSpan PutAttempt()
    {
        lock (_lock) return _serverPut + _serverPut + Slack;
    }

    /// <summary>A transaction's: compile (put timeout), the deadline, the host's two margins, plus slack.</summary>
    internal TimeSpan TxnAttempt(TimeSpan deadline)
    {
        lock (_lock) return _serverPut + deadline + TxnMargins + Slack;
    }

    /// <summary>A read's: the read timeout, twice for a route re-read, plus slack.</summary>
    internal TimeSpan ReadAttempt()
    {
        lock (_lock) return _serverRead + _serverRead + Slack;
    }

    /// <summary>
    /// One logical call, inside <see cref="RdbClientOptions.Timeout"/>. Each attempt first connects; a failed connect
    /// proves nothing was sent, so it is tried again. Each attempt's deadline is <paramref name="attempt"/>, the
    /// server's worst case, so the server answers before the client gives up. A retry starts only when a whole
    /// attempt still fits in the budget; the first attempt always starts, cut at the budget when it is shorter.
    /// </summary>
    internal async Task<T> CallAsync<T>(
        string op, bool isWrite, TimeSpan attempt,
        Func<Pb.Rdb.RdbClient, CallOptions, AsyncUnaryCall<T>> send, CancellationToken ct)
    {
        var watch = Stopwatch.StartNew();
        var budgetEnd = DateTime.UtcNow + _options.Timeout;
        var attempts = 0; // every try, connect probes included
        var sends = 0;    // tries that reached the gateway
        var backoff = TimeSpan.FromMilliseconds(50);
        string? lastNote = null;
        RdbException? lastRefusal = null;

        T Stamp(RdbException ex)
        {
            ex.Attempts = attempts;
            ex.Elapsed = watch.Elapsed;
            throw ex;
        }

        TimeSpan NextPause()
        {
            var p = Jitter(backoff);
            backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 1000));
            return p;
        }

        // Pauses unless the pause would end past the budget; false means the call is out of time.
        async Task<bool> PauseWithin(TimeSpan pause)
        {
            if (DateTime.UtcNow + pause >= budgetEnd) return false;
            if (pause > TimeSpan.Zero) await Pause(pause, ct).ConfigureAwait(false);
            return true;
        }

        for (;;)
        {
            ct.ThrowIfCancellationRequested();
            var left = budgetEnd - DateTime.UtcNow;
            if (left <= TimeSpan.Zero) break;
            attempts++;
            // A failed connect proves nothing was sent, so it is always tried again while time is left.
            var connectFailure = await ConnectAsync(left < ConnectWindow ? left : ConnectWindow, ct).ConfigureAwait(false);
            if (connectFailure is not null)
            {
                lastNote = $"cannot connect to {_endpoint} ({connectFailure.GetType().Name}: {connectFailure.Message})";
                lastRefusal = null;
                DropChannel();
                if (await PauseWithin(NextPause()).ConfigureAwait(false)) continue;
                break;
            }
            left = budgetEnd - DateTime.UtcNow;
            // Never send again unless the whole attempt fits: a cut attempt turns a settleable answer into a lost one.
            if (sends > 0 && left < attempt) break;
            sends++;
            var deadline = DateTime.UtcNow + (left < attempt ? left : attempt);
            try
            {
                using var call = send(ClientFor(), new CallOptions(deadline: deadline, cancellationToken: ct));
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
                var d = RdbErrors.Detail(ex);
                if (d is null)
                {
                    // The server did not decide: the transport lost the call, which may have been sent.
                    if (isWrite)
                    {
                        return Stamp(new RdbUnknownOutcomeException(
                            $"{op}: the answer was lost ({ex.StatusCode}: {ex.Status.Detail}); the write may have applied. " +
                            "next: read the object to see its value",
                            RdbErrorKind.UnknownOutcome, "UNKNOWN_OUTCOME", RdbRetry.Unrecognized, null, null, null, false, ex));
                    }
                    lastNote = $"{op}: the transport lost the call ({ex.StatusCode}: {ex.Status.Detail})";
                    lastRefusal = null;
                    DropChannel();
                    if (await PauseWithin(NextPause()).ConfigureAwait(false)) continue;
                    break;
                }
                var mapped = RdbErrors.Map(ex, d, isWrite);
                if (!RdbErrors.MayRetry(d, isWrite)) return Stamp(mapped);
                lastRefusal = mapped;
                lastNote = ex.Status.Detail;
                var pause = NextPause();
                if (RdbErrors.RetryAfter(d) is { } after && after > pause) pause = after;
                if (DateTime.UtcNow + pause + attempt > budgetEnd) break;
                await Pause(pause, ct).ConfigureAwait(false);
            }
        }
        if (lastRefusal is not null) return Stamp(lastRefusal);
        if (!isWrite && sends > 0)
        {
            // A read that was sent but not answered: "not sent" would be false. A read changes nothing.
            return Stamp(new RdbException(
                $"{op}: no answer within {_options.Timeout.TotalSeconds:0.#} s ({lastNote ?? "no answer"}). A read changes nothing; try again.",
                RdbErrorKind.Unavailable, "UNAVAILABLE", RdbRetry.Unrecognized, true, null, null, null, false));
        }
        return Stamp(new RdbException(
            $"{op}: not sent within {_options.Timeout.TotalSeconds:0.#} s ({lastNote ?? "no attempt made"}). Nothing was applied.",
            RdbErrorKind.Unavailable, "UNAVAILABLE", RdbRetry.Unrecognized, true, null, null, null, false));
    }

    private static TimeSpan Jitter(TimeSpan around) =>
        TimeSpan.FromMilliseconds(around.TotalMilliseconds * (0.5 + Random.Shared.NextDouble()));

    private async Task<Exception?> ConnectAsync(TimeSpan window, CancellationToken ct)
    {
        if (window <= TimeSpan.Zero) return new TimeoutException("no time left to connect");
        if (FakeConnect is { } fake) return fake();
        if (FakeInvoker is not null) return null;
        var ch = Channel();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(window);
        try
        {
            await ch.ConnectAsync(cts.Token).ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            ct.ThrowIfCancellationRequested();
            return ex;
        }
    }

    private Pb.Rdb.RdbClient ClientFor() => FakeInvoker is { } fake ? new(fake) : new(Channel());

    private GrpcChannel Channel()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _channel ??= GrpcChannel.ForAddress(_endpoint, new GrpcChannelOptions
            {
                MaxReceiveMessageSize = 16 * 1024 * 1024,
                MaxSendMessageSize = null,
                HttpHandler = new SocketsHttpHandler
                {
                    ConnectTimeout = ConnectWindow,
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                    KeepAlivePingDelay = TimeSpan.FromSeconds(20),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                    KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                },
                DisposeHttpClient = true,
            });
        }
    }

    /// <summary>Forget the channel so the next attempt connects fresh (a gateway that was down may be back).</summary>
    private void DropChannel()
    {
        GrpcChannel? ch;
        lock (_lock)
        {
            ch = _channel;
            _channel = null;
        }
        ch?.Dispose();
    }

    private static RdbWrite Write(Pb.WriteReply r) => new(r.Version, r.RequestId, r.Generation, (RdbOutcome)(int)r.Outcome);

    private static byte[] Utf8(string s, string name)
    {
        ArgumentNullException.ThrowIfNull(s, name);
        return Encoding.UTF8.GetBytes(s);
    }

    public void Dispose()
    {
        GrpcChannel? ch;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            ch = _channel;
            _channel = null;
        }
        ch?.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
