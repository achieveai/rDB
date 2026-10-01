// A tiny CLI over Retcd.Client. Run `dotnet run -- help`.
using System.Diagnostics;
using Retcd.Client;

const string DefaultEndpoints = "127.0.0.1:17302,127.0.0.1:17312,127.0.0.1:17322";

const string Help = """
    retcd sample: play with a rEtcd cluster from C#

      put <key> <value> [--if-rev N]     store a value (--if-rev 0 = only if new)
      get <key>                          read a value and its revisions
      rm  <key> [--if-rev N]             delete a key
      ls  [prefix|'pattern'] [--page N]  list keys (patterns: * ** ? [a-z])
      dirs <'pattern'>                   keys + one row per sub-folder
      watch [prefix|'pattern'] [--from REV]   stream changes until Ctrl-C (resumes after failover)
      putfile <path> [key]               store a file (max 1 MiB) + meta record
      getfile <key> <out> [--force]      fetch a file, check sha256
      status                             health of each node
      presence-client <name> [--interval-ms N]   send heartbeats until Ctrl-C
      presence-monitor [--interval-ms N]         print who is Up / Late / Down / Back
      bench <n> [--size BYTES]           n sequential puts, prints ops/sec, p50, p99

      --endpoints a:1,b:2,c:3   nodes (or env RETCD_ENDPOINTS; default local cluster at 17300)
      QUOTE patterns in your shell, or it expands them against local files.

    exit codes: 0 ok, 1 error or not found, 2 compare-and-set refused, 3 unknown outcome, 4 unavailable
    """;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    var (pos, flags) = ParseArgs(args);
    if (pos.Count == 0 || pos[0] is "help" or "-h" or "--help")
    {
        Console.WriteLine(Help);
        return 0;
    }
    var cmd = pos[0];
    var rest = pos.Skip(1).ToList();
    var endpoints = (flags.GetValueOrDefault("endpoints") ?? Environment.GetEnvironmentVariable("RETCD_ENDPOINTS") ?? DefaultEndpoints)
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    await using var client = RetcdClient.Create(new RetcdClientOptions { Endpoints = endpoints });
    return await Run(client, cmd, rest, flags, cts.Token);
}
catch (OperationCanceledException)
{
    return 0;
}
catch (CasConflictException ex)
{
    Console.Error.WriteLine($"REFUSED: {ex.Message}");
    if (ex.Exists) Console.Error.WriteLine($"         retry with --if-rev {ex.CurrentRevision}");
    return 2;
}
catch (UnknownOutcomeException ex)
{
    Console.Error.WriteLine($"UNKNOWN OUTCOME: {ex.Message}");
    return 3;
}
catch (RetcdUnavailableException ex)
{
    Console.Error.WriteLine($"unavailable: {ex.Message}");
    return 4;
}
catch (Exception ex) when (ex is RetcdException or ArgumentException or IOException or UsageException)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 1;
}

static async Task<int> Run(RetcdClient client, string cmd, List<string> a, Dictionary<string, string> flags, CancellationToken ct)
{
    switch (cmd)
    {
        case "put":
        {
            Need(a, 2, "put <key> <value> [--if-rev N]");
            var rev = await client.PutAsync(a[0], a[1], IfRev(flags), ct);
            Console.WriteLine($"OK: stored \"{a[0]}\"\n    revision {rev}");
            return 0;
        }
        case "get":
        {
            Need(a, 1, "get <key>");
            var r = await client.GetAsync(a[0], ct);
            if (r is null)
            {
                Console.WriteLine($"not found: \"{a[0]}\"");
                return 1;
            }
            Console.WriteLine($"{r.Key} = {r.ValueAsString()}");
            Console.WriteLine($"    created at revision  {r.CreateRevision}");
            Console.WriteLine($"    modified at revision {r.ModRevision}");
            Console.WriteLine($"    size {r.Value.Length} bytes");
            return 0;
        }
        case "rm":
        {
            Need(a, 1, "rm <key> [--if-rev N]");
            var gone = await client.DeleteAsync(a[0], IfRev(flags), ct);
            Console.WriteLine(gone ? $"OK: removed \"{a[0]}\"" : $"not found: \"{a[0]}\" (nothing removed)");
            return gone ? 0 : 1;
        }
        case "ls":
        {
            var n = 0;
            await foreach (var r in client.ListAsync(a.Count > 0 ? a[0] : "", IntFlag(flags, "page", 500), ct))
            {
                Console.WriteLine($"{r.Key}  ({r.Value.Length} bytes, rev {r.ModRevision})");
                n++;
            }
            Console.WriteLine($"{n} key{(n == 1 ? "" : "s")}");
            return 0;
        }
        case "dirs":
        {
            Need(a, 1, "dirs <'pattern'>");
            var l = await client.ListDirsAsync(a[0], IntFlag(flags, "page", 500), ct);
            foreach (var d in l.Dirs) Console.WriteLine($"{d.Path}  DIR ({d.KeyCount} key{(d.KeyCount == 1 ? "" : "s")})");
            foreach (var f in l.Files) Console.WriteLine($"{f.Key}  ({f.Value.Length} bytes, rev {f.ModRevision})");
            Console.WriteLine($"{l.Files.Count} key(s) + {l.Dirs.Count} dir(s)");
            return 0;
        }
        case "watch":
        {
            var pattern = a.Count > 0 ? a[0] : "";
            ulong? from = flags.TryGetValue("from", out var f) ? ulong.Parse(f) : null;
            Console.WriteLine($"watching {(pattern.Length == 0 ? "everything" : $"\"{pattern}\"")}. Ctrl-C to stop.");
            await foreach (var ev in client.WatchAsync(pattern, from, ct))
            {
                Console.WriteLine(ev.Type == RetcdEventType.Put
                    ? $"rev {ev.Revision}  PUT  {ev.Key}  ({ev.Value.Length} bytes)"
                    : $"rev {ev.Revision}  DEL  {ev.Key}");
            }
            return 0;
        }
        case "putfile":
        {
            if (a.Count is < 1 or > 2) throw new UsageException("putfile <path> [key]");
            var key = a.Count == 2 ? a[1] : "files/" + Path.GetFileName(a[0]);
            var r = await client.PutFileAsync(a[0], key, ct);
            Console.WriteLine($"OK: stored \"{r.Key}\"\n    {r.Size} bytes, revision {r.Revision}\n    sha256 {r.Sha256}\n    meta record at revision {r.MetaRevision}");
            return 0;
        }
        case "getfile":
        {
            Need(a, 2, "getfile <key> <out> [--force]");
            var r = await client.GetFileAsync(a[0], a[1], flags.ContainsKey("force"), ct);
            Console.WriteLine($"OK: wrote {r.Path}\n    {r.Size} bytes, revision {r.Revision}\n    sha256 {r.Sha256}\n    {(r.Verified ? "sha256 matches the meta record" : "no meta record, so sha256 was not checked")}");
            return 0;
        }
        case "status":
        {
            var rows = await client.HealthAsync(ct);
            Console.WriteLine($"{"NODE",-6}{"ENDPOINT",-26}{"STATE",-11}{"ROLE",-10}{"LEADER",-8}{"REV",-8}HASH");
            foreach (var h in rows)
            {
                if (!h.Reachable) Console.WriteLine($"{"-",-6}{h.Endpoint,-26}{"DOWN",-11}({h.Error})");
                else Console.WriteLine($"{h.NodeId,-6}{h.Endpoint,-26}{(h.Ready ? "ready" : "not ready"),-11}{h.Role,-10}{h.CurrentLeader,-8}{h.ClusterRevision,-8}{h.StateHashHex?[..Math.Min(12, h.StateHashHex.Length)]}");
            }
            return rows.Any(h => h.Ready) ? 0 : 1;
        }
        case "presence-client":
        {
            Need(a, 1, "presence-client <name> [--interval-ms N]");
            var interval = TimeSpan.FromMilliseconds(IntFlag(flags, "interval-ms", 1000));
            await using var hb = RetcdPresence.StartHeartbeat(client, a[0], interval);
            hb.BeatFailed += ex => Console.Error.WriteLine($"beat failed: {ex.Message}");
            Console.WriteLine($"sending heartbeats as \"{a[0]}\" every {interval.TotalMilliseconds:0} ms. Ctrl-C to stop (this removes the key).");
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
            }
            Console.WriteLine($"stopped after {hb.Beats} beats ({hb.Failures} failed)");
            return 0;
        }
        case "presence-monitor":
        {
            var interval = TimeSpan.FromMilliseconds(IntFlag(flags, "interval-ms", 1000));
            Console.WriteLine($"watching presence/ (interval {interval.TotalMilliseconds:0} ms; Late after 1.5, Down after 3 missed). Ctrl-C to stop.");
            await foreach (var c in RetcdPresence.WatchAsync(client, interval, 3, ct))
            {
                Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff}  {c.Name,-16} {c.State.ToString().ToUpperInvariant(),-5} (last beat {c.LastSeen.ToLocalTime():HH:mm:ss.fff})");
            }
            return 0;
        }
        case "bench":
        {
            Need(a, 1, "bench <n> [--size BYTES]");
            var n = int.Parse(a[0]);
            var size = IntFlag(flags, "size", 64);
            var value = new byte[size];
            Array.Fill(value, (byte)'x');
            var ms = new List<double>(n);
            Console.WriteLine($"bench: {n} sequential puts of {size} bytes, keys bench/000001...");
            var total = Stopwatch.StartNew();
            for (var i = 1; i <= n; i++)
            {
                var t = Stopwatch.StartNew();
                await client.PutAsync($"bench/{i:D6}", value, null, ct);
                ms.Add(t.Elapsed.TotalMilliseconds);
            }
            total.Stop();
            ms.Sort();
            double Pct(double p) => ms[Math.Min(ms.Count - 1, (int)Math.Ceiling(p * ms.Count) - 1)];
            Console.WriteLine($"  total  {total.Elapsed.TotalSeconds:0.00} s\n  rate   {n / total.Elapsed.TotalSeconds:0} ops/sec\n  p50    {Pct(0.5):0.0} ms\n  p99    {Pct(0.99):0.0} ms\n  max    {ms[^1]:0.0} ms");
            return 0;
        }
        default:
            throw new UsageException($"unknown command \"{cmd}\". try: help");
    }
}

static (List<string> Pos, Dictionary<string, string> Flags) ParseArgs(string[] argv)
{
    var pos = new List<string>();
    var flags = new Dictionary<string, string>();
    var valueFlags = new HashSet<string> { "endpoints", "if-rev", "page", "from", "size", "interval-ms" };
    for (var i = 0; i < argv.Length; i++)
    {
        if (!argv[i].StartsWith("--", StringComparison.Ordinal) || argv[i] == "--help")
        {
            pos.Add(argv[i]);
            continue;
        }
        var name = argv[i][2..];
        if (name == "force") flags[name] = "true";
        else if (valueFlags.Contains(name))
        {
            if (i + 1 >= argv.Length) throw new UsageException($"--{name} needs a value");
            flags[name] = argv[++i];
        }
        else throw new UsageException($"unknown option --{name}. try: help");
    }
    return (pos, flags);
}

static void Need(List<string> a, int n, string usage)
{
    if (a.Count != n) throw new UsageException("usage: " + usage);
}

static ulong? IfRev(Dictionary<string, string> flags) =>
    flags.TryGetValue("if-rev", out var v) ? (ulong.TryParse(v, out var n) ? n : throw new UsageException("--if-rev must be a whole number")) : null;

static int IntFlag(Dictionary<string, string> flags, string name, int dflt) =>
    flags.TryGetValue(name, out var v) ? (int.TryParse(v, out var n) && n > 0 ? n : throw new UsageException($"--{name} must be a positive whole number")) : dflt;

internal sealed class UsageException(string message) : Exception(message);
