using System.Net;
using System.Net.Sockets;

namespace Retcd.Client.Tests;

/// <summary>
/// The one place these tests take a local port. With <c>RETCD_TEST_PORT_RANGE</c> unset it binds port 0, as before.
/// Set to <c>LO-HI</c> (the gate uses 20000-26999), it tries random ports in that range and moves on after any bind
/// failure (in use, 10013, 10048, 10055), so a host whose ephemeral range is exhausted or reserved does not fail
/// the test. After <see cref="Attempts"/> refusals it fails and names the range.
/// </summary>
internal static class TestPorts
{
    public const string RangeVariable = "RETCD_TEST_PORT_RANGE";
    public const int Attempts = 32;

    /// <summary>A started loopback listener. The caller stops it.</summary>
    public static TcpListener Listen() => Listen(Environment.GetEnvironmentVariable(RangeVariable));

    /// <summary>A loopback port that was free a moment ago and has nothing listening on it now.</summary>
    public static int FreeClosedPort()
    {
        var l = Listen();
        var port = Port(l);
        l.Stop();
        return port;
    }

    public static int Port(TcpListener l) => ((IPEndPoint)l.LocalEndpoint).Port;

    internal static TcpListener Listen(string? range)
    {
        if (ParseRange(range) is not var (lo, hi))
        {
            var any = new TcpListener(IPAddress.Loopback, 0);
            any.Start();
            return any;
        }
        SocketException? last = null;
        for (var i = 0; i < Attempts; i++)
        {
            var l = new TcpListener(IPAddress.Loopback, Random.Shared.Next(lo, hi + 1));
            try
            {
                l.Start();
                return l;
            }
            catch (SocketException e)
            {
                last = e;
            }
        }
        throw new InvalidOperationException(
            $"no free loopback port in {RangeVariable}={lo}-{hi} after {Attempts} random tries; last error {last?.SocketErrorCode} ({last?.ErrorCode})",
            last);
    }

    /// <summary>Null when unset or blank. A value that is not <c>LO-HI</c> with 1 &lt;= LO &lt;= HI &lt;= 65535 throws.</summary>
    internal static (int Lo, int Hi)? ParseRange(string? range)
    {
        if (string.IsNullOrWhiteSpace(range)) return null;
        var parts = range.Split('-', StringSplitOptions.TrimEntries);
        if (parts.Length == 2 && int.TryParse(parts[0], out var lo) && int.TryParse(parts[1], out var hi)
            && lo >= 1 && lo <= hi && hi <= 65535)
        {
            return (lo, hi);
        }
        throw new ArgumentException($"{RangeVariable}='{range}' is not LO-HI with 1 <= LO <= HI <= 65535");
    }
}

public class TestPortsTests
{
    [Fact]
    public void Unset_or_blank_means_port_zero_and_a_bad_value_fails_naming_the_variable()
    {
        Assert.Null(TestPorts.ParseRange(null));
        Assert.Null(TestPorts.ParseRange(" "));
        Assert.Equal((20000, 26999), TestPorts.ParseRange("20000-26999"));
        Assert.Equal((5, 5), TestPorts.ParseRange(" 5 - 5 "));
        foreach (var bad in new[] { "20000", "a-b", "9-8", "0-10", "1-70000", "1-2-3" })
        {
            var ex = Assert.Throws<ArgumentException>(() => TestPorts.ParseRange(bad));
            Assert.Contains(TestPorts.RangeVariable, ex.Message);
        }
    }

    [Fact]
    public void A_set_range_binds_inside_it_and_moves_past_a_port_in_use()
    {
        // Hold one port, then ask for a two-port range that includes it: every success must be the other one.
        var held = TestPorts.Listen("20000-26999");
        try
        {
            var p = TestPorts.Port(held);
            Assert.InRange(p, 20000, 26999);
            var (lo, hi) = p < 26999 ? (p, p + 1) : (p - 1, p);
            var other = p == lo ? hi : lo;
            for (var i = 0; i < 5; i++)
            {
                var l = TestPorts.Listen($"{lo}-{hi}");
                try { Assert.Equal(other, TestPorts.Port(l)); }
                finally { l.Stop(); }
            }
        }
        finally
        {
            held.Stop();
        }
    }

    [Fact]
    public void A_range_with_no_free_port_fails_after_the_fixed_count_and_names_the_range()
    {
        var held = TestPorts.Listen("20000-26999");
        try
        {
            var p = TestPorts.Port(held);
            var ex = Assert.Throws<InvalidOperationException>(() => TestPorts.Listen($"{p}-{p}"));
            Assert.Contains($"{TestPorts.RangeVariable}={p}-{p}", ex.Message);
            Assert.Contains($"{TestPorts.Attempts} random tries", ex.Message);
        }
        finally
        {
            held.Stop();
        }
    }
}
