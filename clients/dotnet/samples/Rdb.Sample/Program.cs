// Rdb.Sample: drive rDB from C# through the rdb-server gateway, one command per run.
// `dotnet run --project samples/Rdb.Sample -- --help` prints the commands and exit codes.

using System.Text;
using Rdb.Client;

const string Usage = """
usage: Rdb.Sample [--endpoint URL] [--timeout DUR] [--verbose] COMMAND
  ready [--within DUR]                     does the database admit writes?
  put KEY VALUE [--if-version N]           replace KEY's value
  put --key-file F --value-file F [--if-version N]
  get KEY | get --key-file F               read KEY
  txn KEY=VALUE[@IFVERSION] ... [--deadline DUR]   write all or none
  status REQUEST_ID [GENERATION]           did that write land?
  walk                                     put, get, txn, a refused txn, status: one line each
options:
  --endpoint URL   the gateway (default $RDB_ENDPOINT, else http://127.0.0.1:19190)
  --timeout DUR    the call budget, retries included (default 45s). DUR: 500ms, 2s, 1m, or seconds
  --verbose        print every field of an error
exits: 0 ok; 1 not found; 2 usage; 3 refused by the server (nothing applied);
       4 unknown outcome (may have applied: ask status, or read the object);
       5 not sent or unavailable (nothing applied)
""";

string? endpoint = Environment.GetEnvironmentVariable("RDB_ENDPOINT");
TimeSpan? timeout = null;
var verbose = false;
var rest = new List<string>();
try
{
    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--endpoint": endpoint = Need(args, ref i); break;
            case "--timeout": timeout = Dur(Need(args, ref i)); break;
            case "--verbose": verbose = true; break;
            case "--help" or "-h": Console.WriteLine(Usage); return 0;
            default: rest.Add(args[i]); break;
        }
    }
    if (rest.Count == 0) throw new UsageException("give a command");
}
catch (UsageException e)
{
    Console.Error.WriteLine($"Rdb.Sample: {e.Message}\n{Usage}");
    return 2;
}

var options = new RdbClientOptions();
if (!string.IsNullOrWhiteSpace(endpoint)) options.Endpoint = endpoint;
if (timeout is { } t) options.Timeout = t;
using var client = RdbClient.Create(options);
try
{
    return await Run(client, rest);
}
catch (UsageException e)
{
    Console.Error.WriteLine($"Rdb.Sample: {e.Message}\n{Usage}");
    return 2;
}
catch (RdbException e)
{
    return Fail(e, verbose);
}

static async Task<int> Run(RdbClient client, List<string> a)
{
    var cmd = a[0];
    var flags = Flags(a.Skip(1).ToList(), out var pos);
    switch (cmd)
    {
        case "ready":
        {
            var r = await client.ReadyAsync(flags.TryGetValue("--within", out var w) ? Dur(w) : null);
            Console.WriteLine(r.Ready
                ? $"ready client={r.ClientId} put_timeout_ms={(long)r.PutTimeout.TotalMilliseconds} read_timeout_ms={(long)r.ReadTimeout.TotalMilliseconds}"
                : $"not ready: {r.Detail}");
            return r.Ready ? 0 : 5;
        }
        case "put":
        {
            var key = Key(flags, pos, 0);
            var value = flags.TryGetValue("--value-file", out var vf) ? ReadFile("--value-file", vf)
                : pos.Count > 1 ? Encoding.UTF8.GetBytes(pos[1]) : throw new UsageException("put needs KEY VALUE");
            ulong? ifv = flags.TryGetValue("--if-version", out var iv) ? ULong(iv) : null;
            var r = await client.PutAsync(key, value, ifv);
            Console.WriteLine($"put ok version={r.Version} request_id={r.RequestId} generation={r.Generation} outcome={r.Outcome}");
            return 0;
        }
        case "get":
        {
            var r = await client.GetAsync(Key(flags, pos, 0));
            if (!r.Found)
            {
                Console.WriteLine($"get absent generation={r.Generation} recovery_mode={r.RecoveryMode.ToString().ToLowerInvariant()}");
                return 1;
            }
            Console.WriteLine($"get version={r.Version} generation={r.Generation} recovery_mode={r.RecoveryMode.ToString().ToLowerInvariant()} bytes={r.Value.Length} value={r.ValueAsString()}");
            return 0;
        }
        case "txn":
        {
            if (pos.Count == 0) throw new UsageException("txn needs KEY=VALUE[@IFVERSION] ...");
            var puts = pos.Select(TxnPut).ToList();
            var r = await client.TxnAsync(puts, flags.TryGetValue("--deadline", out var d) ? Dur(d) : null);
            Console.WriteLine($"txn ok version={r.Version} request_id={r.RequestId} generation={r.Generation} outcome={r.Outcome} puts={puts.Count}");
            return 0;
        }
        case "status":
        {
            if (pos.Count < 1) throw new UsageException("status needs REQUEST_ID [GENERATION]");
            var s = await client.StatusAsync(ULong(pos[0]), pos.Count > 1 ? ULong(pos[1]) : null);
            Console.WriteLine(s.Landed == RdbLanded.Yes
                ? $"status Yes version={s.Version} generation={s.Generation} outcome={s.Outcome}"
                : $"status {s.Landed}");
            return 0;
        }
        case "walk":
            return await Walk(client);
        default:
            throw new UsageException($"unknown command {cmd}");
    }
}

// The slice 1 path end to end, one line per step. Status is asked right after the txn, while it is the last write
// sent: each later write carries the ack floor, which lets the server drop the txn's answer (critic M2).
static async Task<int> Walk(RdbClient client)
{
    var ready = await client.ReadyAsync(TimeSpan.FromSeconds(5));
    Console.WriteLine($"walk ready={ready.Ready.ToString().ToLowerInvariant()} client={ready.ClientId}");
    if (!ready.Ready) return 5;
    var p = await client.PutAsync("greeting", "hello");
    Console.WriteLine($"walk put greeting version={p.Version} request_id={p.RequestId} generation={p.Generation}");
    var g = await client.GetAsync("greeting");
    Console.WriteLine($"walk get greeting version={g.Version} value={g.ValueAsString()}");
    var a = await client.GetAsync("acct-a");
    var t = await client.TxnAsync(new[] { new RdbTxnPut("acct-a", "90"), new RdbTxnPut("acct-b", "10") });
    Console.WriteLine($"walk txn acct-a=90 acct-b=10 version={t.Version} request_id={t.RequestId} generation={t.Generation}");
    var s = await client.StatusAsync(t.RequestId, t.Generation);
    Console.WriteLine($"walk status of txn request_id={t.RequestId}: {s.Landed} version={s.Version}");
    try
    {
        // Conditioned on acct-a's version from before the txn: stale, so refused.
        await client.TxnAsync(new[] { new RdbTxnPut("acct-a", "1", a.Found ? a.Version : 1), new RdbTxnPut("acct-b", "2") });
        Console.WriteLine("walk stale txn: applied (unexpected)");
        return 3;
    }
    catch (RdbConditionFailedException e)
    {
        Console.WriteLine($"walk stale txn refused kind={e.KindName} no_mutation={e.NoMutation.ToString().ToLowerInvariant()}");
    }
    var a2 = await client.GetAsync("acct-a");
    var b2 = await client.GetAsync("acct-b");
    Console.WriteLine($"walk after refusal acct-a={a2.ValueAsString()} acct-b={b2.ValueAsString()}");
    return 0;
}

static int Fail(RdbException e, bool verbose)
{
    var code = e switch
    {
        RdbUnknownOutcomeException => 4,
        { FromServer: false } => 5,
        { Kind: RdbErrorKind.Unavailable, NoMutation: true } => 5,
        _ => 3,
    };
    var label = code switch { 4 => "unknown outcome", 5 => "not applied", _ => "refused" };
    Console.WriteLine($"{label} kind={e.KindName} no_mutation={e.NoMutation.ToString().ToLowerInvariant()} request_id={e.RequestId?.ToString() ?? "-"} generation={e.Generation?.ToString() ?? "-"}: {e.Message}");
    if (verbose) Console.WriteLine($"error {e.Describe()}");
    return code;
}

static Dictionary<string, string> Flags(List<string> a, out List<string> positional)
{
    var flags = new Dictionary<string, string>();
    positional = new List<string>();
    for (var i = 0; i < a.Count; i++)
    {
        if (a[i].StartsWith("--", StringComparison.Ordinal))
        {
            if (i + 1 >= a.Count) throw new UsageException($"{a[i]} needs a value");
            flags[a[i]] = a[++i];
        }
        else
        {
            positional.Add(a[i]);
        }
    }
    return flags;
}

// A file the user named that cannot be read is a usage error (exit 2), not a crash (cs-tester D3).
static byte[] ReadFile(string flag, string path)
{
    try { return File.ReadAllBytes(path); }
    catch (Exception e) when (e is IOException or UnauthorizedAccessException)
    {
        throw new UsageException($"{flag} {path}: cannot read it ({e.Message})");
    }
}

static byte[] Key(Dictionary<string, string> flags, List<string> pos, int at) =>
    flags.TryGetValue("--key-file", out var kf) ? ReadFile("--key-file", kf)
    : pos.Count > at ? Encoding.UTF8.GetBytes(pos[at]) : throw new UsageException("give a KEY or --key-file");

static RdbTxnPut TxnPut(string s)
{
    var eq = s.IndexOf('=');
    if (eq <= 0) throw new UsageException($"{s} is not KEY=VALUE[@IFVERSION]");
    var key = s[..eq];
    var value = s[(eq + 1)..];
    ulong? ifv = null;
    var at = value.LastIndexOf('@');
    if (at >= 0)
    {
        ifv = ULong(value[(at + 1)..]);
        value = value[..at];
    }
    return new RdbTxnPut(key, value, ifv);
}

static string Need(string[] a, ref int i) =>
    i + 1 < a.Length ? a[++i] : throw new UsageException($"{a[i]} needs a value");

static ulong ULong(string s) =>
    ulong.TryParse(s, out var v) ? v : throw new UsageException($"{s} is not a whole number");

static TimeSpan Dur(string s)
{
    s = s.Trim();
    if (s.EndsWith("ms", StringComparison.Ordinal) && double.TryParse(s[..^2], out var ms)) return TimeSpan.FromMilliseconds(ms);
    if (s.EndsWith('s') && double.TryParse(s[..^1], out var sec)) return TimeSpan.FromSeconds(sec);
    if (s.EndsWith('m') && double.TryParse(s[..^1], out var min)) return TimeSpan.FromMinutes(min);
    if (double.TryParse(s, out var plain)) return TimeSpan.FromSeconds(plain);
    throw new UsageException($"{s} is not a duration (500ms, 2s, 1m)");
}

sealed class UsageException(string message) : Exception(message);
