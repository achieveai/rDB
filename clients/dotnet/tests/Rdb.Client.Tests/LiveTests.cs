using System.Text;

namespace Rdb.Client.Tests;

/// <summary>
/// A fact that runs against a real gateway: <c>RDB_ENDPOINT</c> names it (scripts/rdb-server-dev.sh up prints it).
/// Without it the test is reported as skipped, with the reason, never passed.
/// </summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public const string Variable = "RDB_ENDPOINT";

    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Variable)))
        {
            Skip = $"needs a gateway: set {Variable} (scripts/rdb-server-dev.sh up prints it)";
        }
    }
}

/// <summary>The C# client through a real rdb-server (cs-tester contracts C4, C6, C7, C9).</summary>
public class LiveTests
{
    private static RdbClient Client() => RdbClient.Create(new RdbClientOptions
    {
        Endpoint = Environment.GetEnvironmentVariable(LiveFactAttribute.Variable)!,
        Timeout = TimeSpan.FromSeconds(30),
    });

    /// <summary>A key no other run has written, so a rerun on the same cluster starts clean.</summary>
    private static string Key(string name) => $"live-{name}-{Guid.NewGuid():N}";

    [LiveFact]
    public async Task Put_then_get_reads_the_value_back_and_the_version_rises()
    {
        using var client = Client();
        Assert.True((await client.ReadyAsync(TimeSpan.FromSeconds(10))).Ready);
        var key = Key("put");
        var first = await client.PutAsync(key, "one");
        var second = await client.PutAsync(key, "two");
        Assert.True(second.Version > first.Version);
        var record = await client.GetAsync(key);
        Assert.Equal("two", record!.ValueAsString());
        Assert.Equal(second.Version, record.Version);
    }

    [LiveFact]
    public async Task C9_an_empty_value_is_not_absent()
    {
        using var client = Client();
        var key = Key("empty");
        await client.PutAsync(key, "");
        var record = await client.GetAsync(key);
        Assert.NotNull(record);
        Assert.Equal(0, record!.Value.Length);
        Assert.Null(await client.GetAsync(Key("absent")));
    }

    [LiveFact]
    public async Task C6_the_key_limit_is_checked_on_the_wire()
    {
        using var client = Client();
        var longest = Encoding.UTF8.GetBytes(Key("k").PadRight(1024, 'k'));
        Assert.Equal(1024, longest.Length);
        await client.PutAsync(longest, Encoding.UTF8.GetBytes("v"));
        var over = new byte[1025];
        Array.Fill(over, (byte)'k');
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync(over, Encoding.UTF8.GetBytes("v")));
        Assert.Equal(RdbErrorKind.InvalidArgument, e.Kind);
        Assert.True(e.NoMutation);
        Assert.True(e.FromServer);
        Assert.DoesNotContain("Db::", e.Message);
    }

    [LiveFact]
    public async Task C7_a_stale_transaction_is_refused_and_changes_nothing()
    {
        using var client = Client();
        var a = Key("acct-a");
        var b = Key("acct-b");
        var t = await client.TxnAsync(new[] { new RdbTxnPut(a, "90"), new RdbTxnPut(b, "10") });
        await Assert.ThrowsAsync<RdbConditionFailedException>(() =>
            client.TxnAsync(new[] { new RdbTxnPut(a, "0", ifVersion: t.Version + 1000), new RdbTxnPut(b, "100") }));
        Assert.Equal("90", (await client.GetAsync(a))!.ValueAsString());
        Assert.Equal("10", (await client.GetAsync(b))!.ValueAsString());
    }

    [LiveFact]
    public async Task C4_status_of_the_last_write_sent_says_yes()
    {
        using var client = Client();
        var t = await client.TxnAsync(new[] { new RdbTxnPut(Key("s"), "1") });
        var status = await client.StatusAsync(t.RequestId, t.Generation);
        Assert.Equal(RdbLanded.Yes, status.Landed);
        Assert.Equal(t.Version, status.Version);
    }
}
