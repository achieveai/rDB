using System.Security.Cryptography;
using System.Text;
using Retcd.Client;

namespace Retcd.Client.Tests;

public class LiveTests
{
    [LiveFact]
    public async Task Put_get_delete_roundtrip()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var rev = await c.PutAsync(p + "k", "héllo");
            var rec = await c.GetAsync(p + "k");
            Assert.NotNull(rec);
            Assert.Equal(p + "k", rec!.Key);
            Assert.Equal("héllo", rec.ValueAsString());
            Assert.Equal(rev, rec.ModRevision);
            Assert.Equal(rev, rec.CreateRevision);

            var bin = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
            await c.PutAsync(p + "bin", bin);
            Assert.Equal(bin, (await c.GetAsync(p + "bin"))!.Value.ToArray());

            var rev2 = await c.PutAsync(p + "k", "v2");
            var rec2 = await c.GetAsync(p + "k");
            Assert.True(rev2 > rev);
            Assert.Equal(rev, rec2!.CreateRevision);   // survives an update
            Assert.Equal(rev2, rec2.ModRevision);

            Assert.True(await c.DeleteAsync(p + "k"));
            Assert.Null(await c.GetAsync(p + "k"));
            Assert.False(await c.DeleteAsync(p + "k"));
            Assert.Null(await c.GetAsync(p + "never"));
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task A_value_of_exactly_1_MiB_roundtrips()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var data = new byte[1024 * 1024];
            RandomNumberGenerator.Fill(data);
            await c.PutAsync(p + "big", data);
            Assert.Equal(data, (await c.GetAsync(p + "big"))!.Value.ToArray());
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task Compare_and_set()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var r1 = await c.PutAsync(p + "k", "1", ifRevision: 0);          // 0 = only if new
            var e0 = await Assert.ThrowsAsync<CasConflictException>(() => c.PutAsync(p + "k", "x", ifRevision: 0));
            Assert.True(e0.Exists);
            Assert.Equal(r1, e0.CurrentRevision);

            var r2 = await c.PutAsync(p + "k", "2", ifRevision: r1);
            var stale = await Assert.ThrowsAsync<CasConflictException>(() => c.PutAsync(p + "k", "3", ifRevision: r1));
            Assert.Equal(r2, stale.CurrentRevision);
            Assert.Equal("2", (await c.GetAsync(p + "k"))!.ValueAsString());   // refused write changed nothing

            var missing = await Assert.ThrowsAsync<CasConflictException>(() => c.PutAsync(p + "nokey", "x", ifRevision: 5));
            Assert.False(missing.Exists);
            Assert.Equal(0UL, missing.CurrentRevision);

            var delStale = await Assert.ThrowsAsync<CasConflictException>(() => c.DeleteAsync(p + "k", ifRevision: r1));
            Assert.Equal(r2, delStale.CurrentRevision);
            Assert.True(await c.DeleteAsync(p + "k", ifRevision: r2));
            Assert.False(await c.DeleteAsync(p + "k", ifRevision: r2));        // gone: not found, not a conflict
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task List_pages_and_globs()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var keys = new[] { "docs/a.md", "docs/b.txt", "docs/sub/c.md", "docs/sub/deep/d.md", "docs/sub/e.txt", "other/x" };
            foreach (var k in keys) await c.PutAsync(p + k, "v");
            for (var i = 0; i < 25; i++) await c.PutAsync($"{p}many/{i:D3}", "v");

            // pages of 7 across 25 keys: all of them, in key order, once each
            var many = new List<string>();
            await foreach (var r in c.ListAsync(p + "many/", pageSize: 7)) many.Add(r.Key);
            Assert.Equal(Enumerable.Range(0, 25).Select(i => $"{p}many/{i:D3}"), many);

            async Task<List<string>> Ls(string pattern)
            {
                var l = new List<string>();
                await foreach (var r in c.ListAsync(p + pattern)) l.Add(r.Key[p.Length..]);
                return l;
            }
            Assert.Equal(new[] { "docs/a.md", "docs/b.txt" }, await Ls("docs/*"));
            Assert.Equal(new[] { "docs/a.md", "docs/sub/c.md", "docs/sub/deep/d.md" }, await Ls("docs/**/*.md"));
            Assert.Equal(new[] { "docs/a.md" }, await Ls("docs/?.md"));
            Assert.Equal(new[] { "docs/a.md", "docs/b.txt" }, await Ls("docs/[a-b].*"));
            Assert.Equal(new[] { "docs/b.txt" }, await Ls("docs/[!a].*"));
            Assert.Equal(5, (await Ls("docs/")).Count);               // plain prefix = everything under it
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task List_dirs_gives_files_and_one_row_per_subfolder()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            foreach (var k in new[] { "docs/a.md", "docs/b.txt", "docs/sub/c.md", "docs/sub/deep/d.md", "docs/sub/e.txt", "docs/zed/f" })
                await c.PutAsync(p + k, "v");

            var l = await c.ListDirsAsync(p + "docs/*");
            Assert.Equal(new[] { p + "docs/a.md", p + "docs/b.txt" }, l.Files.Select(f => f.Key));
            Assert.Equal(new[] { new RetcdDir(p + "docs/sub/", 3), new RetcdDir(p + "docs/zed/", 1) }, l.Dirs);

            var plain = await c.ListDirsAsync(p + "docs");              // a plain prefix is a folder
            Assert.Equal(l.Dirs, plain.Dirs);

            var deep = await c.ListDirsAsync(p + "docs/**/*.md");       // ** means every depth: no dir rows
            Assert.Empty(deep.Dirs);
            Assert.Equal(3, deep.Files.Count);
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task Watch_from_now_sees_put_and_delete_with_revisions()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            await using var w = new Live.Collector(c, p + "w/*");
            // Events before the stream is open are lost by design, so nudge until the first one arrives.
            for (var i = 0; i < 100 && w.Snapshot().Count == 0; i++)
            {
                await c.PutAsync(p + "w/ping", "x");
                await Task.Delay(100);
            }
            var before = w.Snapshot().Count;
            Assert.True(before > 0, "watch never opened");

            var r1 = await c.PutAsync(p + "w/a", "1");
            await c.PutAsync(p + "ignored", "nope");                   // outside the pattern
            await c.PutAsync(p + "w/sub/deeper", "nope");              // '*' does not cross '/'
            await c.DeleteAsync(p + "w/a");
            var events = await w.WaitForAsync(s => s.Count(e => e.Key == p + "w/a") >= 2);

            var a = events.Where(e => e.Key == p + "w/a").ToList();
            Assert.Equal(RetcdEventType.Put, a[0].Type);
            Assert.Equal(r1, a[0].Revision);
            Assert.Equal("1", a[0].ValueAsString());
            Assert.Equal(RetcdEventType.Delete, a[1].Type);
            Assert.True(a[1].Revision > r1);
            Assert.True(a[1].Value.IsEmpty);
            Assert.DoesNotContain(events, e => e.Key.EndsWith("ignored") || e.Key.EndsWith("deeper"));
            Assert.Equal(events.Select(e => e.Revision).OrderBy(x => x), events.Select(e => e.Revision));   // in order
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task Watch_from_a_revision_replays_history()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var r1 = await c.PutAsync(p + "a", "1");
            var r2 = await c.PutAsync(p + "b", "2");
            var r3 = await c.PutAsync(p + "a", "3");

            await using var w = new Live.Collector(c, p, from: r1);     // after r1: expect r2, r3
            var events = await w.WaitForAsync(s => s.Count >= 2);
            Assert.Equal(new[] { r2, r3 }, events.Take(2).Select(e => e.Revision));
            Assert.Equal(new[] { p + "b", p + "a" }, events.Take(2).Select(e => e.Key));
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task Watch_resumes_after_the_connection_breaks_without_loss_or_repeats()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        try
        {
            var start = await c.PutAsync(p + "marker", "m");
            await using var w = new Live.Collector(c, p, from: start);

            var revs = new List<ulong> { await c.PutAsync(p + "k1", "1") };
            await w.WaitForAsync(s => s.Count >= 1);

            c.DropCurrentConnection();                                   // the stream dies under the watcher
            for (var i = 2; i <= 6; i++) revs.Add(await c.PutAsync(p + $"k{i}", "v"));

            var events = await w.WaitForAsync(s => s.Count >= 6);
            await Task.Delay(500);                                       // a repeat would show up now
            events = w.Snapshot();
            Assert.Equal(revs, events.Select(e => e.Revision).ToList());
        }
        finally { await Live.CleanAsync(c, p); }
    }

    [LiveFact]
    public async Task File_roundtrip_checks_sha256_and_catches_tampering()
    {
        await using var c = Live.NewClient();
        var p = Live.Prefix();
        var dir = Directory.CreateTempSubdirectory("retcd-test-");
        try
        {
            var src = Path.Combine(dir.FullName, "in.bin");
            var data = new byte[300_000];
            RandomNumberGenerator.Fill(data);
            await File.WriteAllBytesAsync(src, data);

            var put = await c.PutFileAsync(src, p + "f.bin");
            Assert.Equal(300_000, put.Size);
            Assert.Equal(Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant(), put.Sha256);

            // the meta record is the kv.mjs format
            var meta = (await c.GetAsync("meta/" + p + "f.bin"))!.ValueAsString();
            Assert.Equal($"{{\"size\":300000,\"sha256\":\"{put.Sha256}\",\"stored_at_rev\":{put.Revision}}}",
                meta.Replace(" ", ""), ignoreCase: false);

            var dst = Path.Combine(dir.FullName, "out.bin");
            var got = await c.GetFileAsync(p + "f.bin", dst);
            Assert.True(got.Verified);
            Assert.Equal(data, await File.ReadAllBytesAsync(dst));

            await Assert.ThrowsAsync<IOException>(() => c.GetFileAsync(p + "f.bin", dst));       // no silent overwrite
            await c.GetFileAsync(p + "f.bin", dst, overwrite: true);

            await c.PutAsync(p + "f.bin", "tampered");                                           // bytes no longer match meta
            var dst2 = Path.Combine(dir.FullName, "bad.bin");
            await Assert.ThrowsAsync<ChecksumMismatchException>(() => c.GetFileAsync(p + "f.bin", dst2));
            Assert.False(File.Exists(dst2));

            await c.PutAsync(p + "nometa", "plain");                                             // no meta: written, not verified
            var noMeta = await c.GetFileAsync(p + "nometa", Path.Combine(dir.FullName, "nm.txt"));
            Assert.False(noMeta.Verified);

            await Assert.ThrowsAsync<RetcdNotFoundException>(() => c.GetFileAsync(p + "absent", Path.Combine(dir.FullName, "x")));
        }
        finally
        {
            await Live.CleanAsync(c, p);
            await Live.CleanAsync(c, "meta/" + p);
            dir.Delete(true);
        }
    }

    [LiveFact]
    public async Task Health_lists_every_node_and_one_leader()
    {
        await using var c = Live.NewClient();
        var rows = await c.HealthAsync();
        Assert.Equal(Live.Endpoints!.Length, rows.Count);
        Assert.All(rows, r => Assert.True(r.Reachable, r.Error));
        Assert.Single(rows, r => r.Role == "leader");
        Assert.All(rows, r => Assert.True(r.Ready));
    }

    [LiveFact]
    public async Task Presence_detects_a_silent_client_and_a_clean_goodbye()
    {
        await using var c = Live.NewClient();
        var name = "t-" + Guid.NewGuid().ToString("N")[..8];
        var interval = TimeSpan.FromMilliseconds(200);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var seen = new List<PresenceChange>();
        var changed = new SemaphoreSlim(0);
        var monitor = Task.Run(async () =>
        {
            try
            {
                await foreach (var ch in RetcdPresence.WatchAsync(c, interval, 3, cts.Token))
                {
                    if (ch.Name != name) continue;
                    lock (seen) seen.Add(ch);
                    changed.Release();
                }
            }
            catch (OperationCanceledException) { }
        });

        async Task<PresenceChange> Next()
        {
            if (!await changed.WaitAsync(TimeSpan.FromSeconds(15))) throw new TimeoutException("no presence change; saw " + string.Join(",", seen.Select(s => s.State)));
            lock (seen) return seen[^1];
        }

        try
        {
            // 1. alive, then it goes silent without saying goodbye (a crash): Up -> Late -> Down
            var hb = RetcdPresence.StartHeartbeat(c, name, interval, removeOnDispose: false);
            Assert.Equal(PresenceState.Up, (await Next()).State);
            var beat = (await c.GetAsync("presence/" + name))!.ValueAsString();
            Assert.Matches("^\\{\"name\":\"" + name + "\",\"pid\":\\d+,\"seq\":\\d+,\"sent_at\":\"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z\"\\}$", beat);
            await hb.DisposeAsync();
            Assert.Equal(PresenceState.Late, (await Next()).State);
            Assert.Equal(PresenceState.Down, (await Next()).State);

            // 2. it comes back: Back
            var hb2 = RetcdPresence.StartHeartbeat(c, name, interval, removeOnDispose: true);
            Assert.Equal(PresenceState.Back, (await Next()).State);

            // 3. clean goodbye deletes the key: Down at once, long before 3 intervals
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await hb2.DisposeAsync();
            Assert.Equal(PresenceState.Down, (await Next()).State);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"goodbye took {sw.Elapsed}");
            Assert.Null(await c.GetAsync("presence/" + name));
        }
        finally
        {
            cts.Cancel();
            await monitor;
            await c.DeleteAsync("presence/" + name);
        }
    }

    [LiveFact]
    public async Task Presence_monitor_reports_clients_already_alive_as_Up()
    {
        await using var c = Live.NewClient();
        var name = "t-" + Guid.NewGuid().ToString("N")[..8];
        var interval = TimeSpan.FromMilliseconds(300);
        await using (var hb = RetcdPresence.StartHeartbeat(c, name, interval))
        {
            await Task.Delay(700);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var ch in RetcdPresence.WatchAsync(c, interval, 3, cts.Token))
            {
                if (ch.Name != name) continue;
                Assert.Equal(PresenceState.Up, ch.State);
                break;
            }
        }
    }
}

