using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Pb = Retcd.V1;

namespace Retcd.Client;

public sealed partial class RetcdClient
{
    /// <summary>
    /// Stream changes under a prefix or pattern. Starts after <paramref name="fromRevision"/>
    /// (null = from now, 0 = from the start of retained history).
    /// If the stream breaks (leader change, node stopped, network) it reconnects and resumes after the
    /// last revision it saw, so no event is lost or repeated. It ends only when you cancel, or throws
    /// <see cref="RevisionCompactedException"/> if the resume point was compacted away.
    /// A node allows 100 open watches per principal.
    /// </summary>
    public async IAsyncEnumerable<RetcdEvent> WatchAsync(
        string patternOrPrefix, ulong? fromRevision = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var prefix = RetcdGlob.LiteralPrefix(patternOrPrefix);
        var re = RetcdGlob.HasGlob(patternOrPrefix) ? RetcdGlob.ToRegex(patternOrPrefix) : null;
        var last = fromRevision ?? await ReadRevisionAsync(ct).ConfigureAwait(false);
        var slowToConnect = new HashSet<string>(); // nodes that missed the quick connect window since the last message

        for (;;)
        {
            ct.ThrowIfCancellationRequested();
            var endpoint = _transport.Current;
            // Connect first, with the same quick window as a call, so a dead node costs QuickConnect and not the
            // transport's connect timeout. This matters most when fromRevision is given and the watch is the first
            // thing this client does. A node that is already connected returns at once.
            var quick = !_transport.Endpoints.All(slowToConnect.Contains);
            var window = quick ? QuickConnect : SlowConnect;
            if (await _transport.ConnectAsync(endpoint, DateTime.UtcNow + window, window, ct).ConfigureAwait(false) is not null)
            {
                slowToConnect.Add(endpoint);
                _transport.Drop(endpoint);
                _transport.Rotate();
                if (!quick) await Task.Delay(_options.WatchReconnectDelay, ct).ConfigureAwait(false);
                continue;
            }
            var req = new Pb.WatchRequest { Prefix = ByteString.CopyFromUtf8(prefix), StartAfterRevision = last };
            RpcException? failure = null;
            using var call = _transport.ClientFor(endpoint).Watch(req, new CallOptions(cancellationToken: ct));
            var stream = call.ResponseStream;
            for (;;)
            {
                Pb.WatchResponse msg;
                try
                {
                    if (!await stream.MoveNext(ct).ConfigureAwait(false)) break; // server closed the stream
                    msg = stream.Current;
                }
                catch (RpcException ex)
                {
                    failure = ex;
                    break;
                }

                slowToConnect.Clear();
                if (msg.BodyCase == Pb.WatchResponse.BodyOneofCase.Progress)
                {
                    if (msg.Progress.Revision > last) last = msg.Progress.Revision; // quiet heartbeat, no key data
                    continue;
                }
                if (msg.BodyCase != Pb.WatchResponse.BodyOneofCase.Event) continue;

                var ev = msg.Event;
                if (ev.Revision <= last) continue; // already delivered before a reconnect
                last = ev.Revision;
                var key = ev.Key.ToStringUtf8();
                if (re is not null && !re.IsMatch(key)) continue;
                yield return ev.ChangeCase == Pb.Event.ChangeOneofCase.Put
                    ? new RetcdEvent(RetcdEventType.Put, key, ev.Put.Value.Memory, ev.Revision)
                    : new RetcdEvent(RetcdEventType.Delete, key, ReadOnlyMemory<byte>.Empty, ev.Revision);
            }

            ct.ThrowIfCancellationRequested();
            if (failure is not null) await HandleWatchFailureAsync(failure, endpoint, ct).ConfigureAwait(false);
            await Task.Delay(_options.WatchReconnectDelay, ct).ConfigureAwait(false);
        }
    }

    /// <summary>Decide whether a broken watch can resume. Returns when it can; throws when it cannot.</summary>
    private async Task HandleWatchFailureAsync(RpcException ex, string endpoint, CancellationToken ct)
    {
        if (ex.StatusCode == StatusCode.Cancelled) ct.ThrowIfCancellationRequested();
        if (ex.StatusCode == StatusCode.OutOfRange) throw new RevisionCompactedException(RetcdErrors.MinRevision(ex), ex);
        if (ex.StatusCode == StatusCode.ResourceExhausted && !RetcdErrors.Resumable(ex))
        {
            throw new RetcdException($"watch refused: {ex.Status.Detail} (over a server limit; not resumable)", ex, ex.StatusCode);
        }

        if (RetcdErrors.IsNotLeader(ex))
        {
            var hint = RetcdErrors.LeaderHint(ex);
            if (hint is not null && _transport.Normalize(hint) != endpoint) _transport.Use(hint);
            else _transport.Rotate();
            return;
        }
        if (RetcdErrors.IsConnectFailure(ex))
        {
            _transport.Drop(endpoint);
            _transport.Rotate();
            return;
        }
        if (ex.StatusCode is StatusCode.Unavailable or StatusCode.ResourceExhausted or StatusCode.Unknown or StatusCode.Aborted or StatusCode.Internal or StatusCode.Cancelled)
        {
            // The stream broke, or the server dropped a slow watcher (resumable). A watch has no outcome to doubt: resume.
            _transport.Rotate();
            return;
        }
        throw RetcdErrors.Map(ex, "watch", isWrite: false);
    }

    /// <summary>The cluster's current read revision, learned with a Get of a key that does not exist.</summary>
    private async Task<ulong> ReadRevisionAsync(CancellationToken ct)
    {
        var r = await UnaryAsync("watch", isWrite: false, 0,
            (c, o) => c.GetAsync(new Pb.GetRequest { Key = ByteString.CopyFromUtf8(NowProbeKey) }, o), ct).ConfigureAwait(false);
        return r.ReadRevision;
    }
}
