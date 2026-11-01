using Grpc.Core;
using Pb = Rdb.V1;

namespace Rdb.Client.Tests;

/// <summary>
/// One row per guard of the retry loop whose removal left every other row green (review F-009), and the
/// budget rule of F-002. Each comment names the guard the row goes red without.
/// </summary>
public class GuardTests
{
    private static (RdbClient Client, ScriptedInvoker Gateway, List<TimeSpan> Pauses) Client(TimeSpan timeout)
    {
        var client = RdbClient.Create(new RdbClientOptions { Timeout = timeout });
        var gateway = new ScriptedInvoker();
        var pauses = new List<TimeSpan>();
        client.FakeInvoker = gateway;
        client.Pause = (pause, _) =>
        {
            pauses.Add(pause);
            return Task.CompletedTask;
        };
        return (client, gateway, pauses);
    }

    // Guard: a server's retry_after lengthens the pause (RdbErrors.RetryAfter in CallAsync).
    [Fact]
    public async Task A_refusal_with_retry_after_waits_at_least_that_long()
    {
        var (client, gateway, pauses) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.ResourceExhausted, Pb.ErrorKind.Overloaded, "OVERLOADED",
            Pb.RetryRule.BoundedJitter, noMutation: true, retryAfterMs: 700)).Then(Wire.Written());
        await client.PutAsync("k", "v");
        Assert.Equal(2, gateway.Calls.Count);
        Assert.Single(pauses);
        Assert.True(pauses[0] >= TimeSpan.FromMilliseconds(700), $"paused {pauses[0]}");
    }

    // Guard: a kind this client does not know is never retried (RetryableRule's Unrecognized exclusion).
    [Fact]
    public async Task A_refusal_of_an_unknown_kind_is_not_sent_again()
    {
        var (client, gateway, _) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.Unavailable, (Pb.ErrorKind)99, "FROM_A_NEWER_SERVER",
            Pb.RetryRule.BoundedJitter, noMutation: true));
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.Unrecognized, e.Kind);
        Assert.Equal("FROM_A_NEWER_SERVER", e.KindName);
        Assert.Single(gateway.Calls);
    }

    // Guard: RefreshRoute is a retryable rule (RetryableRule).
    [Fact]
    public async Task A_not_primary_refusal_that_changed_nothing_is_sent_again()
    {
        var (client, gateway, _) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.Unavailable, Pb.ErrorKind.NotPrimary, "NOT_PRIMARY",
            Pb.RetryRule.RefreshRoute, noMutation: true)).Then(Wire.Written(requestId: 31));
        var written = await client.PutAsync("k", "v");
        Assert.Equal(31UL, written.RequestId);
        Assert.Equal(2, gateway.Calls.Count);
    }

    // Guard: no pause is taken when the pause and a whole attempt would end past the budget. A put's attempt is
    // 12 s; after a 2 s retry_after it does not fit 13 s.
    [Fact]
    public async Task A_retry_that_would_end_past_the_budget_is_not_waited_for()
    {
        var (client, gateway, pauses) = Client(TimeSpan.FromSeconds(13));
        gateway.Then(Wire.Refusal(StatusCode.ResourceExhausted, Pb.ErrorKind.Overloaded, "OVERLOADED",
            Pb.RetryRule.BoundedJitter, noMutation: true, retryAfterMs: 2_000));
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.Overloaded, e.Kind);
        Assert.Single(gateway.Calls);
        Assert.Empty(pauses);
    }

    // Guard: a send starts only when a whole attempt fits, the first one included (F-002). A put's attempt is
    // 300 ms here; a connect that fails after 100 ms leaves about 250 ms of a 350 ms budget, so nothing is sent.
    [Fact]
    public async Task A_send_that_no_longer_fits_after_a_slow_connect_is_not_started()
    {
        var (client, gateway, _) = Client(TimeSpan.FromMilliseconds(350));
        client.UseServerTimeouts(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50));
        client.Slack = TimeSpan.FromMilliseconds(100);
        var refusals = 1;
        client.FakeConnect = () =>
        {
            if (refusals-- <= 0) return null;
            Thread.Sleep(100);
            return new HttpRequestException("connection refused");
        };
        gateway.Then(Wire.Written());
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.Unavailable, e.Kind);
        Assert.True(e.NoMutation);
        Assert.Contains("not sent", e.Message);
        Assert.Empty(gateway.Calls);
    }

    // D4 (A-R474): a budget of exactly one attempt passes the up-front check, but connecting spends part of it, so
    // the send no longer fits. It ends "not sent" and says why. A put's attempt is 300 ms here; the connect takes 50.
    [Fact]
    public async Task A_timeout_of_exactly_one_attempt_is_not_sent_once_connecting_has_spent_some_of_it()
    {
        var (client, gateway, _) = Client(TimeSpan.FromMilliseconds(300));
        client.UseServerTimeouts(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50));
        client.Slack = TimeSpan.FromMilliseconds(100);
        client.FakeConnect = () =>
        {
            Thread.Sleep(50);
            return null;
        };
        gateway.Then(Wire.Written());
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.Unavailable, e.Kind);
        Assert.True(e.NoMutation);
        Assert.Contains("not sent", e.Message);
        Assert.Contains("time left after connecting", e.Message);
        Assert.Empty(gateway.Calls);
    }

    // F-002: a budget shorter than one attempt is refused before anything is sent.
    [Fact]
    public async Task A_timeout_shorter_than_one_attempt_is_refused_before_sending()
    {
        var (client, gateway, _) = Client(TimeSpan.FromSeconds(5));
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.InvalidArgument, e.Kind);
        Assert.Equal(RdbRetry.Definitive, e.Retry);
        Assert.True(e.NoMutation);
        Assert.False(e.FromServer);
        Assert.Contains("nothing was sent", e.Message);
        Assert.Contains("at least 13 s (one attempt plus 1 s to connect)", e.Message);
        Assert.Empty(gateway.Calls);
    }

    // F-002 and F-011: the attempt sizes are the server's real waits, and the default budget fits the largest call.
    [Fact]
    public void The_default_timeout_fits_every_call_and_the_attempts_are_the_servers_waits()
    {
        using var client = RdbClient.Create(new RdbClientOptions());
        Assert.Equal(TimeSpan.FromSeconds(12), client.PutAttempt());
        Assert.Equal(TimeSpan.FromSeconds(4), client.ReadAttempt());
        Assert.Equal(TimeSpan.FromSeconds(13), client.TxnAttempt(TimeSpan.FromSeconds(5)));
        Assert.Equal(TimeSpan.FromSeconds(38), client.TxnAttempt(TimeSpan.FromSeconds(30)));
        Assert.True(new RdbClientOptions().Timeout >= client.TxnAttempt(TimeSpan.FromSeconds(30)));
    }
}
