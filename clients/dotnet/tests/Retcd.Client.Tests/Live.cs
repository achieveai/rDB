using System.Diagnostics;
using Retcd.Client;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Retcd.Client.Tests;

/// <summary>
/// Live tests run only when RETCD_ENDPOINTS is set, for example
/// RETCD_ENDPOINTS=127.0.0.1:17602,127.0.0.1:17612,127.0.0.1:17622. Otherwise they show as skipped.
/// </summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (Live.Endpoints is null) Skip = "set RETCD_ENDPOINTS (comma-separated host:port) to run live tests";
    }
}

/// <summary>
/// Needs RETCD_ENDPOINTS and RETCD_CLUSTER_DIR (a dir made by scripts/local-cluster.sh) and a Git Bash on PATH.
/// It kills one node of that cluster and starts it again. Never point it at a cluster you do not own.
/// </summary>
public sealed class ClusterFactAttribute : FactAttribute
{
    public ClusterFactAttribute()
    {
        if (Live.Endpoints is null) Skip = "set RETCD_ENDPOINTS to run live tests";
        else if (Live.ClusterDir is null) Skip = "set RETCD_CLUSTER_DIR (and CARGO_TARGET_DIR) to run tests that stop a node";
    }
}

internal static class Live
{
    public static string[]? Endpoints
    {
        get
        {
            var v = Environment.GetEnvironmentVariable("RETCD_ENDPOINTS");
            return string.IsNullOrWhiteSpace(v) ? null : v.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
    }

    public static string? ClusterDir
    {
        get
        {
            var v = Environment.GetEnvironmentVariable("RETCD_CLUSTER_DIR");
            return string.IsNullOrWhiteSpace(v) ? null : v;
        }
    }

    public static RetcdClient NewClient(TimeSpan? timeout = null, TimeSpan? reconnect = null) =>
        RetcdClient.Create(new RetcdClientOptions
        {
            Endpoints = Endpoints!,
            Timeout = timeout ?? TimeSpan.FromSeconds(20),
            WatchReconnectDelay = reconnect ?? TimeSpan.FromMilliseconds(200),
        });

    public static string Prefix() => $"t/{Guid.NewGuid():N}/";

    public static async Task CleanAsync(RetcdClient c, string prefix)
    {
        var keys = new List<string>();
        await foreach (var r in c.ListAsync(prefix)) keys.Add(r.Key);
        foreach (var k in keys) await c.DeleteAsync(k);
    }

    /// <summary>Runs a watch in the background and collects what it sees.</summary>
    public sealed class Collector : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _pump;
        private readonly object _lock = new();
        private readonly List<RetcdEvent> _events = new();

        public Collector(RetcdClient c, string pattern, ulong? from = null)
        {
            _pump = Task.Run(async () =>
            {
                try
                {
                    await foreach (var ev in c.WatchAsync(pattern, from, _cts.Token))
                    {
                        lock (_lock) _events.Add(ev);
                    }
                }
                catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                {
                }
            });
        }

        public List<RetcdEvent> Snapshot()
        {
            lock (_lock) return _events.ToList();
        }

        public async Task<List<RetcdEvent>> WaitForAsync(Func<List<RetcdEvent>, bool> done, double seconds = 20)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed.TotalSeconds < seconds)
            {
                if (_pump.IsFaulted) await _pump; // surface the watch's own error
                var s = Snapshot();
                if (done(s)) return s;
                await Task.Delay(50);
            }
            throw new TimeoutException($"watch did not see what the test expected within {seconds}s; saw {Snapshot().Count} events: " +
                string.Join(", ", Snapshot().Select(e => $"{e.Revision}:{e.Type}:{e.Key}")));
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await _pump;
            _cts.Dispose();
        }
    }
}
