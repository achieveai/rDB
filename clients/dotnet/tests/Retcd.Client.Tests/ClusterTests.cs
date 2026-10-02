using System.Diagnostics;
using System.Runtime.ExceptionServices;
using Retcd.Client;

namespace Retcd.Client.Tests;

/// <summary>
/// Stops the leader of the cluster named by RETCD_CLUSTER_DIR (a path .NET can open, like C:/rdb_test_data/x)
/// and starts it again with samples/retcd-playground/node.sh. CARGO_TARGET_DIR must point at the server build.
/// </summary>
public class ClusterTests
{
    private static string RepoNodeScript()
    {
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
        {
            var f = Path.Combine(d.FullName, "samples", "retcd-playground", "node.sh");
            if (File.Exists(f)) return f.Replace('\\', '/');
        }
        throw new FileNotFoundException("samples/retcd-playground/node.sh not found above " + AppContext.BaseDirectory);
    }

    /// <summary>Git Bash. Plain "bash" can be WSL on Windows, which cannot see these paths. Override with RETCD_BASH.</summary>
    private static string BashExe()
    {
        var set = Environment.GetEnvironmentVariable("RETCD_BASH");
        if (!string.IsNullOrWhiteSpace(set)) return set;
        const string gitBash = @"C:\Program Files\Git\bin\bash.exe";
        return File.Exists(gitBash) ? gitBash : "bash";
    }

    private static string Bash(string script)
    {
        var psi = new ProcessStartInfo(BashExe(), new[] { "-c", script }) { RedirectStandardOutput = true, RedirectStandardError = true };
        using var p = Process.Start(psi)!;
        var text = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
        p.WaitForExit();
        if (p.ExitCode != 0) throw new InvalidOperationException($"bash failed ({p.ExitCode}): {text}");
        return text;
    }

    [ClusterFact]
    public async Task Calls_and_a_watch_follow_the_leader_when_it_is_stopped()
    {
        var dir = Live.ClusterDir!.Replace('\\', '/');
        await using var c = Live.NewClient(TimeSpan.FromSeconds(30));
        var p = Live.Prefix();
        var leader = Assert.Single(await c.HealthAsync(), h => h.Role == "leader");
        var leaderNode = (int)leader.NodeId!.Value;
        Exception? failure = null;
        try
        {
            var start = await c.PutAsync(p + "marker", "m");
            await using var w = new Live.Collector(c, p, from: start);
            var before = await c.PutAsync(p + "before", "1");
            await w.WaitForAsync(s => s.Any(e => e.Key == p + "before"));

            // Same stop file local-cluster.sh and node.sh use. A node will not finish stopping while a
            // watch is open on it, so break the client's connection until it is gone (a rolling restart).
            File.WriteAllText(Path.Combine(dir, $"node-{leaderNode}", "stop"), "");
            var sw = Stopwatch.StartNew();
            while ((await c.HealthAsync()).Single(h => h.Endpoint == leader.Endpoint).Reachable)
            {
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(60), "the node did not stop");
                c.DropCurrentConnection();
                await Task.Delay(300);
            }

            // writes keep working through the new leader, and are applied exactly once
            var after = await c.PutAsync(p + "after", "2");
            Assert.True(after > before);
            Assert.Equal("2", (await c.GetAsync(p + "after"))!.ValueAsString());

            // the watch resumed by itself and saw the write made after the stop, with nothing repeated
            var events = await w.WaitForAsync(s => s.Any(e => e.Key == p + "after"), seconds: 30);
            Assert.Equal(new[] { p + "before", p + "after" }, events.Select(e => e.Key).ToArray());

            var now = await c.HealthAsync();
            var newLeader = Assert.Single(now, h => h.Role == "leader");
            Assert.NotEqual((ulong)leaderNode, newLeader.NodeId);
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        try
        {
            Bash($"bash '{RepoNodeScript()}' start {leaderNode} --dir '{dir}'");
            for (var i = 0; i < 150; i++)
            {
                var h = await c.HealthAsync();
                if (h.All(x => x.Reachable && x.Ready)) break;
                await Task.Delay(200);
            }
            await Live.CleanAsync(c, p);
        }
        catch (Exception ex) when (failure is not null)
        {
            Console.Error.WriteLine("restart/cleanup also failed: " + ex.Message); // the test's own failure stays primary
        }
        if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
    }

}
