using System.Net;
using System.Net.Sockets;
using Grpc.Core;
using Retcd.Client;

namespace Retcd.Client.Tests;

/// <summary>Behaviour that needs no cluster: checks before sending, and what a dead address looks like.</summary>
public class ClientBehaviorTests
{
    private static int FreeClosedPort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private static RetcdClient DeadClient(double seconds = 3) =>
        RetcdClient.Create(new RetcdClientOptions
        {
            Endpoints = new[] { $"127.0.0.1:{FreeClosedPort()}" },
            Timeout = TimeSpan.FromSeconds(seconds),
        });

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
    // The node is a bare TcpListener: the client's connect step passes, and the scripted answer stands in
    // for what the server would have sent.

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

    private static async Task WithListeningNode(double timeoutSeconds, Func<RetcdClient, Task> body)
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        try
        {
            await using var c = RetcdClient.Create(new RetcdClientOptions
            {
                Endpoints = new[] { $"127.0.0.1:{((IPEndPoint)l.LocalEndpoint).Port}" },
                Timeout = TimeSpan.FromSeconds(timeoutSeconds),
            });
            await body(c);
        }
        finally
        {
            l.Stop();
        }
    }

    [Fact]
    public async Task No_leader_yet_is_waited_out_for_a_read_and_a_write_because_nothing_was_applied()
    {
        await WithListeningNode(10, async c =>
        {
            foreach (var isWrite in new[] { false, true })
            {
                var attempts = 0;
                var r = await c.UnaryAsync("put", isWrite, 0, (_, _) => Answer(() =>
                    ++attempts < 3 ? throw Status(StatusCode.Unavailable, stamped: true) : "applied"), CancellationToken.None);
                Assert.Equal("applied", r);
                Assert.Equal(3, attempts);
            }
        });
    }

    [Fact]
    public async Task No_leader_yet_gives_up_at_the_call_timeout_and_says_nothing_was_applied()
    {
        await WithListeningNode(1, async c =>
        {
            var attempts = 0;
            var started = DateTime.UtcNow;
            var ex = await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.UnaryAsync("put", true, 0,
                (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: true); }), CancellationToken.None));
            var took = DateTime.UtcNow - started;
            Assert.Contains("no leader known yet", ex.Message);
            Assert.Contains("Nothing was applied", ex.Message);
            Assert.True(attempts > 1, $"retried ({attempts} attempts)");
            Assert.InRange(took.TotalSeconds, 0.9, 5);
        });
    }

    [Fact]
    public async Task Unavailable_with_a_reason_or_without_the_stamp_is_not_resent()
    {
        await WithListeningNode(10, async c =>
        {
            var attempts = 0;
            await Assert.ThrowsAsync<RetcdUnavailableException>(() => c.UnaryAsync("get", false, 0,
                (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: true, reason: "feature_not_activated"); }), CancellationToken.None));
            Assert.Equal(1, attempts);

            attempts = 0; // a write the transport lost: it may have been applied, so it is never resent
            await Assert.ThrowsAsync<UnknownOutcomeException>(() => c.UnaryAsync("put", true, 0,
                (_, _) => Answer(() => { attempts++; throw Status(StatusCode.Unavailable, stamped: false); }), CancellationToken.None));
            Assert.Equal(1, attempts);
        });
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
}
