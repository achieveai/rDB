using System.Net.Sockets;
using Google.Protobuf;
using Grpc.Core;
using Retcd.Client;

namespace Retcd.Client.Tests;

/// <summary>Behaviour that needs no cluster: checks before sending, and what a dead address looks like.</summary>
public class ClientBehaviorTests
{
    // A dead node with no sockets: the scripted connect step is refused every time, so nothing is ever sent and the
    // test takes no host port to find a closed one. Health has no connect seam, so it asks port 0, where nothing can
    // listen (the client refuses port 0 as a node endpoint, so the node itself cannot use it).
    private static RetcdClient DeadClient(double seconds = 3)
    {
        var c = RetcdClient.Create(new RetcdClientOptions
        {
            Endpoints = new[] { "127.0.0.1:1" },
            HealthEndpoints = new[] { "127.0.0.1:0" },
            Timeout = TimeSpan.FromSeconds(seconds),
        });
        c.TestTransport.FakeConnect = (_, _) => new SocketException((int)SocketError.ConnectionRefused);
        return c;
    }

    [Fact]
    public async Task Value_over_1_MiB_is_refused_before_anything_is_sent()
    {
        await using var c = DeadClient(30);
        var ex = await Assert.ThrowsAsync<ValueTooLargeException>(() => c.PutAsync("k", new byte[1024 * 1024 + 1]));
        Assert.Equal(1024 * 1024 + 1, ex.Size);
        Assert.Equal(1024 * 1024, ex.Limit);
    }

    [Fact]
    public async Task Exactly_1_MiB_is_not_refused_by_the_client()
    {
        await using var c = DeadClient(1);
        // Passes the size check, then fails because nothing is listening: unavailable, not too large.
        await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.PutAsync("k", new byte[1024 * 1024]));
    }

    [Fact]
    public async Task Bad_keys_are_argument_errors()
    {
        await using var c = DeadClient(30);
        await Assert.ThrowsAsync<ArgumentException>(() => c.GetAsync(""));
        await Assert.ThrowsAsync<ArgumentException>(() => c.PutAsync(new string('k', 1025), "v"));
        await Assert.ThrowsAsync<ArgumentException>(() => c.DeleteAsync(""));
        // 1024 bytes exactly is fine for the check (it then fails to connect)
    }

    [Fact]
    public async Task Bad_page_size_is_an_argument_error()
    {
        await using var c = DeadClient(30);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => { await foreach (var _ in c.ListAsync("a/", 0)) { } });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => { await foreach (var _ in c.ListAsync("a/", 1001)) { } });
    }

    [Fact]
    public async Task Nobody_listening_is_unavailable_for_a_write_never_unknown_outcome()
    {
        // Nothing was sent, so the write is known not to have happened. This is the line between
        // "cannot connect" (safe) and "sent, then timed out" (unknown).
        await using var c = DeadClient(3);
        var put = c.PutAsync("k", "v");
        var get = c.GetAsync("k");
        var del = c.DeleteAsync("k");
        var ex = await Assert.ThrowsAsync<RetcdUnavailableException>(() => put);
        Assert.Contains("Nothing was applied", ex.Message);
        await Assert.ThrowsAsync<RetcdUnavailableException>(() => get);
        await Assert.ThrowsAsync<RetcdUnavailableException>(() => del);
    }

    // ---- the retry loop, with each answer scripted in place of the RPC ----------------------------
    // No sockets: ScriptedClient fakes the connect step, so it passes without the host, and the scripted answer
    // stands in for what the server would have sent.

    private static RpcException Status(StatusCode code, bool stamped, string? reason = null)
    {
        var md = new Metadata();
        if (stamped) md.Add("retcd-outcome", "rejected");
        if (reason is not null) md.Add("retcd-reason", reason);
        return new RpcException(new Grpc.Core.Status(code, "scripted"), md);
    }

    private static AsyncUnaryCall<string> Answer(Func<string> answer)
    {
        Task<string> task;
        try { task = Task.FromResult(answer()); }
        catch (Exception ex) { task = Task.FromException<string>(ex); }
        return new AsyncUnaryCall<string>(task, Task.FromResult(new Metadata()), () => Grpc.Core.Status.DefaultSuccess, () => new Metadata(), () => { });
    }

    [Fact]
    public async Task No_leader_yet_is_waited_out_for_a_read_and_a_write_because_nothing_was_applied()
    {
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(10));
        foreach (var isWrite in new[] { false, true })
        {
            var attempts = 0;
            var r = await c.UnaryAsync("put", isWrite, 0, (_, _) => Answer(() =>
                ++attempts < 3 ? throw Status(StatusCode.Unavailable, stamped: true) : "applied"), CancellationToken.None);
            Assert.Equal("applied", r);
            Assert.Equal(3, attempts);
        }
    }

    [Fact]
    public async Task No_leader_yet_gives_up_at_the_call_timeout_and_says_nothing_was_applied()
    {
        // Scripted connect: the whole 1 s budget goes to "no leader yet" answers and their pauses, never to a socket
        // connect on a busy host, so the call retries and ends at Timeout (the loop and PauseWithin both wait it out).
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(1));
        var attempts = 0;
        var started = DateTime.UtcNow;
        var ex = await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.UnaryAsync("put", true, 0,
            (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: true); }), CancellationToken.None));
        var took = DateTime.UtcNow - started;
        Assert.Contains("no leader known yet", ex.Message);
        Assert.Contains("Nothing was applied", ex.Message);
        Assert.True(attempts > 1, $"retried ({attempts} attempts)");
        Assert.InRange(took.TotalSeconds, 0.9, 5);
    }

    [Fact]
    public async Task Unavailable_with_a_reason_or_without_the_stamp_is_not_resent()
    {
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(10));
        var attempts = 0;
        await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.UnaryAsync("get", false, 0,
            (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: true, reason: "feature_not_activated"); }), CancellationToken.None));
        Assert.Equal(1, attempts);

        attempts = 0; // a write the transport lost: it may have been applied, so it is never resent
        await Assert.ThrowsAsync<UnknownOutcomeException>(() => c.UnaryAsync("put", true, 0,
            (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: false); }), CancellationToken.None));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task A_read_the_transport_lost_is_resent_because_a_read_changes_nothing()
    {
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(10));
        var attempts = 0;
        var r = await c.UnaryAsync("get", false, 0, (_, _) => Answer(() =>
            ++attempts < 3 ? throw Status(StatusCode.Unavailable, stamped: false) : "read"), CancellationToken.None);
        Assert.Equal("read", r);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task A_read_the_transport_keeps_losing_gives_up_at_the_call_timeout()
    {
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(1));
        var attempts = 0;
        var started = DateTime.UtcNow;
        var ex = await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.UnaryAsync("get", false, 0,
            (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: false); }), CancellationToken.None));
        Assert.Contains("dropped the call", ex.Message);
        Assert.True(attempts > 1, $"retried ({attempts} attempts)");
        Assert.InRange((DateTime.UtcNow - started).TotalSeconds, 0.9, 5);
    }

    [Fact]
    public async Task A_cancelled_token_stops_the_call()
    {
        await using var c = DeadClient(30);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => c.GetAsync("k", cts.Token));
    }

    [Fact]
    public async Task Cancelling_during_the_wait_for_a_node_ends_the_call_quickly()
    {
        await using var c = DeadClient(30);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => c.PutAsync("k", "v", null, cts.Token));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"took {sw.Elapsed}");
    }

    [Fact]
    public async Task Health_of_a_dead_node_is_a_row_not_an_exception()
    {
        await using var c = DeadClient(30);
        var rows = await c.HealthAsync();
        var row = Assert.Single(rows);
        Assert.False(row.Reachable);
        Assert.False(string.IsNullOrEmpty(row.Error));
    }

    // ---- PR #1 review rows -----------------------------------------------------------------------

    [Fact]
    public async Task R1_F001_a_refusal_seen_after_the_connect_step_is_unknown_for_a_write_and_never_resent()
    {
        await using var c = ScriptedClient(1, TimeSpan.FromSeconds(2));
        // The connect step passed, so this came from the send. Its socket error is no proof the write was not sent.
        var refused = Refused();
        var attempts = 0;
        var ex = await Assert.ThrowsAsync<UnknownOutcomeException>(() => c.UnaryAsync("put", true, 0,
            (_, _) => Answer(() => { attempts++; throw refused; }), CancellationToken.None));
        Assert.Equal(1, attempts);
        Assert.DoesNotContain("Nothing was", ex.Message);

        attempts = 0; // a read with the same error still moves on: a read changes nothing
        var r = await c.UnaryAsync("get", false, 0, (_, _) => Answer(() => ++attempts < 2 ? throw refused : "read"), CancellationToken.None);
        Assert.Equal("read", r);
    }

    /// <summary>A connect failure as the call itself reports it, after the connect step passed.</summary>
    private static RpcException Refused() => new(new Grpc.Core.Status(StatusCode.Unavailable, "Error connecting to subchannel.",
        new HttpRequestException("refused", new SocketException((int)SocketError.ConnectionRefused))));

    private static int Port(string endpoint) => new Uri(endpoint).Port;

    [Fact]
    public async Task R1_F010_a_dead_node_costs_one_quick_window_and_the_slow_window_comes_only_after_every_node_missed()
    {
        var (c, _) = MemoryNode.Attach(nodes: 3);
        await using var _c = c;
        var log = new List<string>();
        var node = c.TestTransport.FakeInvoker!;
        c.TestTransport.FakeInvoker = ep => { log.Add($"send {Port(ep)}"); return node(ep); };

        // Node 1 never connects: the write goes to node 2 after one 400 ms window, and nothing is sent to node 1.
        c.TestTransport.FakeConnect = (ep, window) =>
        {
            log.Add($"connect {Port(ep)} {window.TotalMilliseconds}");
            return Port(ep) == 1 ? new TimeoutException("scripted: no answer") : null;
        };
        await c.PutAsync("k", "v");
        Assert.Equal(new[] { "connect 1 400", "connect 2 400", "send 2" }, log);
        Assert.Equal(2, Port(c.CurrentEndpoint));

        // Every node is slow: each gets the quick window once, and only then one gets the 3 s window.
        log.Clear();
        c.TestTransport.FakeConnect = (ep, window) =>
        {
            log.Add($"connect {Port(ep)} {window.TotalMilliseconds}");
            return window < TimeSpan.FromSeconds(3) ? new TimeoutException("scripted: slow") : null;
        };
        Assert.NotNull(await c.GetAsync("k"));
        Assert.Equal(new[] { "connect 2 400", "connect 3 400", "connect 1 400", "connect 2 3000", "send 2" }, log);
    }

    [Fact]
    public async Task R2_F014_ListDirsAsync_stops_at_its_byte_limit_across_pages_and_a_small_folder_is_unchanged()
    {
        var (c, node) = MemoryNode.Attach();
        await using var _c = c;
        var mib = ByteString.CopyFrom(new byte[1024 * 1024]); // one value shared by every record: the fake stays small
        var pages = 0;
        node.ListOverride = req =>
        {
            if (++pages > 20) throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, "walked far past the limit"));
            var start = req.PageToken.Length > 0 ? int.Parse(req.PageToken.ToStringUtf8()) : 0;
            var resp = new Retcd.V1.ListResponse { ReadRevision = 1, NextPageToken = ByteString.CopyFromUtf8((start + 10).ToString()) };
            for (var i = start; i < start + 10; i++)
            {
                resp.Records.Add(new Retcd.V1.Record { Key = ByteString.CopyFromUtf8($"big/f{i:D4}"), Value = mib, CreateRevision = 1, ModRevision = 1 });
            }
            return resp; // never ends by itself
        };
        var ex = await Assert.ThrowsAsync<ResultTooLargeException>(() => c.ListDirsAsync("big", pageSize: 10));
        Assert.Contains("ListAsync", ex.Message);
        Assert.Equal(7, pages); // stopped on the page that crossed 64 MiB, not after the walk
        Assert.Equal(64L * 1024 * 1024, ex.Limit);
        Assert.True(ex.Size > ex.Limit);
        Assert.Null(ex.GrpcStatus); // the client stopped; the server refused nothing

        // A small folder over several pages comes back as before.
        node.ListOverride = null;
        foreach (var k in new[] { "small/a", "small/b", "small/sub/c", "small/sub/d", "small/z/e" }) await c.PutAsync(k, "v");
        var l = await c.ListDirsAsync("small", pageSize: 2);
        Assert.Equal(new[] { "small/a", "small/b" }, l.Files.Select(f => f.Key));
        Assert.Equal(new[] { ("small/sub/", 2), ("small/z/", 1) }, l.Dirs.Select(d => (d.Path, d.KeyCount)));
    }

    [Fact]
    public async Task R2_F006_two_non_utf8_keys_stay_distinct_and_KeyBytes_addresses_each_one()
    {
        var (c, node) = MemoryNode.Attach();
        await using var _c = c;
        await c.PutAsync(new byte[] { 0x80 }, "eighty"u8.ToArray());
        await c.PutAsync(new byte[] { 0x81 }, "eighty-one"u8.ToArray());

        var under80 = new List<RetcdRecord>();
        await foreach (var r in c.ListAsync(new byte[] { 0x80 })) under80.Add(r);
        Assert.Equal(new byte[] { 0x80 }, Assert.Single(under80).KeyBytes.ToArray()); // a byte prefix lists only its own keys

        var all = new List<RetcdRecord>();
        await foreach (var r in c.ListAsync("")) all.Add(r);
        Assert.Equal(2, all.Count);
        Assert.Equal(all[0].Key, all[1].Key); // the string form is lossy: both are U+FFFD
        Assert.Equal(new byte[] { 0x80 }, all[0].KeyBytes.ToArray());
        Assert.Equal(new byte[] { 0x81 }, all[1].KeyBytes.ToArray());

        // Each record is addressed again by its returned bytes.
        Assert.Equal("eighty", (await c.GetAsync(all[0].KeyBytes))!.ValueAsString());
        Assert.Equal("eighty-one", (await c.GetAsync(all[1].KeyBytes))!.ValueAsString());
        await c.PutAsync(all[0].KeyBytes, "changed"u8.ToArray(), ifRevision: all[0].ModRevision);
        Assert.True(await c.DeleteAsync(all[1].KeyBytes));
        Assert.Equal(new[] { "80" }, node.Keys.Select(Convert.ToHexString)); // 0x81 deleted, nothing landed at EF BF BD
        Assert.Equal("changed", (await c.GetAsync(new byte[] { 0x80 }))!.ValueAsString());

        // Watch events carry the exact key too.
        var seen = new List<RetcdEvent>();
        await foreach (var ev in c.WatchAsync("", fromRevision: 0))
        {
            seen.Add(ev);
            if (seen.Count == 4) break;
        }
        Assert.Equal(new[] { "80", "81", "80", "81" }, seen.Select(e => Convert.ToHexString(e.KeyBytes.Span)));
        Assert.Equal(RetcdEventType.Delete, seen[3].Type);

        // A record made by hand gets the UTF-8 bytes of its Key.
        Assert.Equal("ab"u8.ToArray(), new RetcdRecord("ab", default, 0, 0).KeyBytes.ToArray());
    }

    [Fact]
    public async Task R2_F014_the_ListDirsAsync_limit_counts_key_value_and_folder_path_bytes_to_the_byte()
    {
        var limit = (int)RetcdClient.MaxListDirsBytes;
        var (c, node) = MemoryNode.Attach();
        await using var _c = c;
        static Retcd.V1.ListResponse Page(params (string Key, int ValueBytes)[] recs)
        {
            var resp = new Retcd.V1.ListResponse { ReadRevision = 1 };
            foreach (var (k, n) in recs)
            {
                resp.Records.Add(new Retcd.V1.Record { Key = ByteString.CopyFromUtf8(k), Value = ByteString.CopyFrom(new byte[n]), CreateRevision = 1, ModRevision = 1 });
            }
            return resp;
        }

        // "d/k" is 3 bytes, so a value of limit - 3 is exactly the limit.
        node.ListOverride = _ => Page(("d/k", limit - 3));
        Assert.Single((await c.ListDirsAsync("d")).Files);
        node.ListOverride = _ => Page(("d/k", limit - 2)); // one byte over: the key bytes count
        await Assert.ThrowsAsync<ResultTooLargeException>(() => c.ListDirsAsync("d"));
        // "d/sub/z" is not a file of d/*, but it adds the folder row "d/sub/" (6 bytes), which tips it over.
        node.ListOverride = _ => Page(("d/k", limit - 3), ("d/sub/z", 0));
        await Assert.ThrowsAsync<ResultTooLargeException>(() => c.ListDirsAsync("d"));
    }

    [Fact]
    public async Task R2_F006_with_on_Key_or_KeyBytes_keeps_the_two_in_step()
    {
        var (c, _) = MemoryNode.Attach();
        await using var _c = c;
        await c.PutAsync(new byte[] { 0x80 }, "v"u8.ToArray());
        var rec = (await c.GetAsync(new byte[] { 0x80 }))!;
        Assert.Equal("�", rec.Key);

        var renamed = rec with { Key = "plain" };
        Assert.Equal("plain", renamed.Key);
        Assert.Equal("plain"u8.ToArray(), renamed.KeyBytes.ToArray()); // not the old 0x80
        var rebytes = rec with { KeyBytes = "a/b"u8.ToArray() };
        Assert.Equal("a/b", rebytes.Key); // not the old U+FFFD
        Assert.Equal(new byte[] { 0x80 }, rec.KeyBytes.ToArray()); // the original is untouched

        var ev = new RetcdEvent(RetcdEventType.Put, "k", default, 1) { KeyBytes = new byte[] { 0x81 } };
        Assert.Equal("�", ev.Key);
        Assert.Equal("y"u8.ToArray(), (ev with { Key = "y" }).KeyBytes.ToArray());
        Assert.Equal("z", (ev with { KeyBytes = "z"u8.ToArray() }).Key);

        // Records made by hand from the same key still compare equal.
        Assert.Equal(new RetcdRecord("x", default, 1, 1), new RetcdRecord("x", default, 1, 1));
        Assert.Equal(new RetcdEvent(RetcdEventType.Delete, "x", default, 1), new RetcdEvent(RetcdEventType.Delete, "x", default, 1));
    }

    [Fact]
    public void A_R2_5_records_and_events_compare_keys_by_content_not_by_buffer()
    {
        // The client builds each event over its own key buffer, so two deliveries of one delete hold equal bytes in
        // different arrays. They must still compare equal, as they did when only the Key text was compared.
        RetcdEvent Del(byte[] key, ulong rev) => new(RetcdEventType.Delete, "", ReadOnlyMemory<byte>.Empty, rev) { KeyBytes = key };
        var a = Del(new byte[] { 0x61, 0x80 }, 7);
        var b = Del(new byte[] { 0x61, 0x80 }, 7);
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, Del(new byte[] { 0x61, 0x81 }, 7)); // same Key text (a + U+FFFD), a different key
        Assert.NotEqual(a, Del(new byte[] { 0x61, 0x80 }, 8));

        RetcdRecord Rec(byte[] key) => new("", ReadOnlyMemory<byte>.Empty, 1, 2) { KeyBytes = key };
        Assert.Equal(Rec(new byte[] { 0x62 }), Rec(new byte[] { 0x62 }));
        Assert.Equal(Rec(new byte[] { 0x62 }).GetHashCode(), Rec(new byte[] { 0x62 }).GetHashCode());
        Assert.NotEqual(Rec(new byte[] { 0x62 }), Rec(new byte[] { 0x63 }));
        // A key given as text equals the same key given as bytes.
        Assert.Equal(new RetcdRecord("b", ReadOnlyMemory<byte>.Empty, 1, 2), Rec("b"u8.ToArray()));
        Assert.Equal(new RetcdRecord("b", ReadOnlyMemory<byte>.Empty, 1, 2).GetHashCode(), Rec("b"u8.ToArray()).GetHashCode());
    }

    // ---- A1: a pause after a failure stays inside Timeout ----------------------------------------
    // No sockets: ScriptedClient fakes the connect step, and the scripted answer stands in for the RPC.

    private static RetcdClient ScriptedClient(int nodes, TimeSpan timeout)
    {
        var c = RetcdClient.Create(new RetcdClientOptions
        {
            Endpoints = Enumerable.Range(1, nodes).Select(i => $"127.0.0.1:{i}").ToArray(),
            Timeout = timeout,
        });
        c.TestTransport.FakeConnect = (_, _) => null;
        return c;
    }

    // Each row below makes its failure land after the deadline, so no attempt can follow it and any pause after it
    // is pure lateness. Capped, the call gives up at once; uncapped, it first sleeps the whole pause. A row times only
    // that tail, from the failure to the give-up, so a stall before the failure on a busy host cannot fail it. There
    // is no whole-call bound: the time before the failure is the scripted step's own sleep, and the tail bounds the rest.

    private const double TailBoundMs = 50;

    /// <summary>
    /// Runs <paramref name="call"/> on a client from <paramref name="newClient"/>. The call must fail late and call its
    /// <c>Action</c> at the moment of the failure. Returns the shortest time from that failure to the give-up over up to
    /// three tries. One stall on a busy host cannot fail a row, but an uncapped pause is in every try. Each try gets a
    /// fresh client, so every try is that client's first late give-up and nothing one try leaves behind (the current
    /// node, any state the client keeps) can let a later try skip the pause.
    /// </summary>
    private static async Task<(double Ms, RetcdUnavailableException Ex)> TailAfterALateFailure(
        Func<RetcdClient> newClient, Func<RetcdClient, Action, Task> call)
    {
        var best = (Ms: double.MaxValue, Ex: (RetcdUnavailableException)null!);
        for (var i = 0; i < 3 && best.Ms >= TailBoundMs; i++)
        {
            await using var c = newClient();
            long failedAt = 0;
            var ex = await Assert.ThrowsAsync<RetcdUnavailableException>(() => call(c, () => failedAt = System.Diagnostics.Stopwatch.GetTimestamp()));
            var ms = System.Diagnostics.Stopwatch.GetElapsedTime(failedAt).TotalMilliseconds;
            Assert.NotEqual(0, failedAt);
            if (ms < best.Ms) best = (ms, ex);
        }
        return best;
    }

    [Fact]
    public async Task A1_a_no_leader_pause_that_would_run_past_Timeout_does_not_make_the_call_late()
    {
        // Timeout 100 ms. The one attempt takes 150 ms and answers "no leader yet": the 200 ms election pause must not run.
        var attemptsPerTry = new List<int>();
        var (ms, ex) = await TailAfterALateFailure(() => ScriptedClient(1, TimeSpan.FromMilliseconds(100)), async (c, failed) =>
        {
            var attempts = 0;
            try
            {
                await c.UnaryAsync("put", true, 0, (_, _) => Answer(() =>
                {
                    attempts++;
                    Thread.Sleep(150);
                    failed();
                    throw Status(StatusCode.Unavailable, stamped: true);
                }), CancellationToken.None);
            }
            finally { attemptsPerTry.Add(attempts); }
        });
        Assert.All(attemptsPerTry, n => Assert.Equal(1, n)); // no attempt after the budget
        Assert.Contains("no leader known yet", ex.Message);
        Assert.True(ms < TailBoundMs, $"ran {ms:0} ms past a no-leader answer that came after the deadline");
    }

    [Fact]
    public async Task A1_a_hint_loop_pause_that_would_run_past_Timeout_does_not_make_the_call_late()
    {
        // Two nodes naming each other: two free hops, then the third send takes 350 ms against a 300 ms Timeout, so
        // the third hop's 200 ms pause must not run. The wide Timeout leaves the two free hops 300 ms to happen in.
        RpcException NotLeader(string hint) => new(new Grpc.Core.Status(StatusCode.FailedPrecondition, "not leader"),
            new Metadata { { "retcd-outcome", "rejected" }, { "retcd-leader-endpoint", hint } });
        var sendsPerTry = new List<int>();
        var (ms, _) = await TailAfterALateFailure(() => ScriptedClient(2, TimeSpan.FromMilliseconds(300)), async (c, failed) =>
        {
            var sends = 0;
            try
            {
                await c.UnaryAsync("put", true, 0, (_, _) => Answer(() =>
                {
                    if (++sends == 3)
                    {
                        Thread.Sleep(350);
                        failed();
                    }
                    throw NotLeader(sends % 2 == 1 ? "127.0.0.1:2" : "127.0.0.1:1");
                }), CancellationToken.None);
            }
            finally { sendsPerTry.Add(sends); }
        });
        Assert.All(sendsPerTry, n => Assert.Equal(3, n));
        Assert.True(ms < TailBoundMs, $"ran {ms:0} ms past a third hop that came after the deadline");
    }

    // The 100 ms pauses after a connect failure, a call-connect failure and a dropped read. In each row the failure
    // lands after the deadline (Timeout 100 ms, the scripted step takes 150 ms), so no attempt can follow it. Capped,
    // the call gives up at once; uncapped, it first sleeps the whole 100 ms. The row times only that tail.

    [Fact]
    public async Task A1_a_pause_after_a_connect_failure_does_not_run_past_Timeout()
    {
        // One node: the quick window fails at once, then the slow window fails late and is followed by the 100 ms pause.
        var (ms, ex) = await TailAfterALateFailure(() => ScriptedClient(1, TimeSpan.FromMilliseconds(100)), async (c, failed) =>
        {
            var connects = 0;
            c.TestTransport.FakeConnect = (_, window) =>
            {
                if (++connects == 2)
                {
                    Assert.Equal(TimeSpan.FromSeconds(3), window);
                    Thread.Sleep(150);
                    failed();
                }
                return new TimeoutException("scripted: no answer");
            };
            await c.GetAsync("k");
        });
        Assert.Contains("cannot connect", ex.Message);
        Assert.True(ms < TailBoundMs, $"ran {ms:0} ms past a connect failure that came after the deadline");
    }

    [Fact]
    public async Task A1_a_pause_after_a_call_connect_failure_does_not_run_past_Timeout()
    {
        var (ms, ex) = await TailAfterALateFailure(() => ScriptedClient(1, TimeSpan.FromMilliseconds(100)), (c, failed) => c.UnaryAsync("get", false, 0,
            (_, _) => Answer(() => { Thread.Sleep(150); failed(); throw Refused(); }), CancellationToken.None));
        Assert.Contains("cannot connect", ex.Message);
        Assert.True(ms < TailBoundMs, $"ran {ms:0} ms past a call-connect failure that came after the deadline");
    }

    [Fact]
    public async Task A1_a_pause_after_a_dropped_read_does_not_run_past_Timeout()
    {
        var (ms, ex) = await TailAfterALateFailure(() => ScriptedClient(1, TimeSpan.FromMilliseconds(100)), (c, failed) => c.UnaryAsync("get", false, 0,
            (_, _) => Answer(() => { Thread.Sleep(150); failed(); throw Status(StatusCode.Unavailable, stamped: false); }), CancellationToken.None));
        Assert.Contains("dropped the call", ex.Message);
        Assert.True(ms < TailBoundMs, $"ran {ms:0} ms past a dropped read that came after the deadline");
    }
}
