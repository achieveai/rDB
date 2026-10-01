using System.Net;
using System.Net.Sockets;
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
