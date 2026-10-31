using Grpc.Core;
using Pb = Rdb.V1;

namespace Rdb.Client.Tests;

/// <summary>
/// The client's retry rule and error mapping against a scripted gateway (cs-tester contracts C1-C4, C7).
/// No network: <see cref="RdbClient.FakeInvoker"/> answers, and pauses take no time unless a test needs them to.
/// </summary>
public class RetryRuleTests
{
    private static (RdbClient Client, ScriptedInvoker Gateway) Client(TimeSpan timeout, bool instantPauses = true)
    {
        var client = RdbClient.Create(new RdbClientOptions { Timeout = timeout });
        var gateway = new ScriptedInvoker();
        client.FakeInvoker = gateway;
        if (instantPauses) client.Pause = (_, _) => Task.CompletedTask;
        return (client, gateway);
    }

    /// <summary>Server timeouts of 100 ms (put) and 50 ms (read) and 100 ms of slack: a put's attempt is 300 ms, a read's 200 ms.</summary>
    private static void Shrink(RdbClient client)
    {
        client.UseServerTimeouts(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50));
        client.Slack = TimeSpan.FromMilliseconds(100);
    }

    private static RpcException Paused(bool noMutation, ulong? requestId = null) =>
        Wire.Refusal(StatusCode.Unavailable, Pb.ErrorKind.ProtectionPaused, "PROTECTION_PAUSED",
            Pb.RetryRule.BoundedJitter, noMutation, requestId);

    // C1: a write is sent again only on a server-decided refusal that changed nothing, and it applies once.
    [Fact]
    public async Task C1_a_write_refused_with_no_mutation_is_sent_again_until_it_lands()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Paused(true)).Then(Paused(true)).Then(Paused(true)).Then(Wire.Written(requestId: 14));
        var written = await client.PutAsync("k", "v");
        Assert.Equal(14UL, written.RequestId);
        Assert.Equal(4, gateway.Calls.Count);
    }

    [Fact]
    public async Task C1_a_write_refused_that_may_have_applied_is_never_sent_again()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Paused(false, requestId: 21));
        var e = await Assert.ThrowsAsync<RdbUnknownOutcomeException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.ProtectionPaused, e.Kind);
        Assert.Equal(21UL, e.RequestId);
        Assert.True(e.FromServer);
        Assert.Single(gateway.Calls);
    }

    [Fact]
    public async Task C1_a_definitive_refusal_is_never_sent_again()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.InvalidArgument, Pb.ErrorKind.InvalidArgument, "INVALID_ARGUMENT",
            Pb.RetryRule.Definitive, noMutation: true));
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.InvalidArgument, e.Kind);
        Assert.True(e.NoMutation);
        Assert.Single(gateway.Calls);
    }

    // C1: bounded by time. A put's attempt is 300 ms here; after a pause of at least 25 ms a second one does not fit 320 ms.
    [Fact]
    public async Task C1_a_retry_starts_only_when_a_whole_attempt_fits_the_budget()
    {
        var (client, gateway) = Client(TimeSpan.FromMilliseconds(320));
        Shrink(client);
        gateway.Then(Paused(true));
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.ProtectionPaused, e.Kind);
        Assert.True(e.NoMutation);
        Assert.Single(gateway.Calls);
    }

    // C2: no answer is an unknown outcome, never sent again; no request id when the transport lost it.
    [Fact]
    public async Task C2_a_write_whose_answer_was_lost_is_unknown_with_no_id_and_not_resent()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Lost(StatusCode.DeadlineExceeded));
        var e = await Assert.ThrowsAsync<RdbUnknownOutcomeException>(() => client.PutAsync("k", "v"));
        Assert.Null(e.RequestId);
        Assert.False(e.FromServer);
        Assert.False(e.NoMutation);
        Assert.Equal(RdbRetry.Reconcile, e.Retry);
        Assert.Contains("no answer before the call's deadline", e.Message);
        Assert.DoesNotContain("DeadlineExceeded: )", e.Message);
        Assert.Single(gateway.Calls);
    }

    [Fact]
    public async Task C2_a_server_unknown_outcome_carries_the_id_to_ask_status_with()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.Unknown, Pb.ErrorKind.UnknownOutcome, "UNKNOWN_OUTCOME",
            Pb.RetryRule.QueryStatus, noMutation: false, requestId: 9));
        var e = await Assert.ThrowsAsync<RdbUnknownOutcomeException>(() => client.TxnAsync(new[] { new RdbTxnPut("a", "1") }));
        Assert.Equal(9UL, e.RequestId);
        Assert.True(e.FromServer);
        Assert.Single(gateway.Calls);
    }

    [Fact]
    public async Task C2_a_read_the_transport_lost_is_read_again()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Lost()).Then(Wire.Found("v"));
        var record = await client.GetAsync("k");
        Assert.Equal("v", record!.ValueAsString());
        Assert.Equal(2, gateway.Calls.Count);
    }

    // C3: a write that cannot connect was not sent, so it is tried again, and lands once.
    [Fact]
    public async Task C3_a_write_that_could_not_connect_is_tried_again_and_lands_once()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        var refusals = 2;
        client.FakeConnect = () => refusals-- > 0 ? new HttpRequestException("connection refused") : null;
        gateway.Then(Wire.Written());
        await client.PutAsync("k", "v");
        Assert.Single(gateway.Calls);
    }

    [Fact]
    public async Task C3_a_write_that_never_connects_is_not_sent_and_says_why()
    {
        var (client, gateway) = Client(TimeSpan.FromMilliseconds(400), instantPauses: false);
        Shrink(client); // a put's attempt is 300 ms
        client.FakeConnect = () => new TaskCanceledException("A task was canceled.");
        var e = await Assert.ThrowsAsync<RdbException>(() => client.PutAsync("k", "v"));
        Assert.Equal(RdbErrorKind.Unavailable, e.Kind);
        Assert.False(e.FromServer);
        Assert.True(e.NoMutation);
        Assert.Equal(RdbRetry.BoundedJitter, e.Retry);
        Assert.Contains("not sent", e.Message);
        Assert.Contains("no connection within", e.Message);
        Assert.DoesNotContain("A task was canceled", e.Message);
        Assert.True(e.Attempts >= 1);
        Assert.Empty(gateway.Calls);
    }

    // C4: status never says "no".
    [Theory]
    [InlineData((int)Pb.StatusState.Resolved, RdbLanded.Yes)]
    [InlineData((int)Pb.StatusState.Expired, RdbLanded.ReadTheObject)]
    [InlineData((int)Pb.StatusState.Unknown, RdbLanded.NotKnown)]
    [InlineData((int)Pb.StatusState.Unspecified, RdbLanded.NotKnown)]
    public async Task C4_status_answers_yes_read_the_object_or_not_known(int wireState, RdbLanded landed)
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        var state = (Pb.StatusState)wireState; // the wire enum is internal, so the theory takes its number
        gateway.Then(new Pb.StatusReply { State = state, Version = state == Pb.StatusState.Resolved ? 5UL : 0UL });
        var status = await client.StatusAsync(11, 2);
        Assert.Equal(landed, status.Landed);
    }

    // C7: a refused conditional write is its own exception, changed nothing, and is not sent again.
    [Fact]
    public async Task C7_condition_failed_is_typed_changed_nothing_and_is_not_resent()
    {
        var (client, gateway) = Client(TimeSpan.FromSeconds(60));
        gateway.Then(Wire.Refusal(StatusCode.FailedPrecondition, Pb.ErrorKind.ConditionFailed, "CONDITION_FAILED",
            Pb.RetryRule.Definitive, noMutation: true));
        var e = await Assert.ThrowsAsync<RdbConditionFailedException>(() => client.PutAsync("k", "v", ifVersion: 2));
        Assert.True(e.NoMutation);
        Assert.Single(gateway.Calls);
    }

    // A read sent and never answered does not claim it was not sent.
    [Fact]
    public async Task A_read_with_no_answer_says_so_not_that_it_was_not_sent()
    {
        // A read's attempt is 200 ms here and the loss takes 150 ms, so in a 300 ms budget it is not sent again.
        var (client, gateway) = Client(TimeSpan.FromMilliseconds(300));
        Shrink(client);
        gateway.ThenAfter(TimeSpan.FromMilliseconds(150), Wire.Lost());
        var e = await Assert.ThrowsAsync<RdbException>(() => client.GetAsync("k"));
        Assert.Single(gateway.Calls);
        Assert.Contains("no answer within", e.Message);
        Assert.DoesNotContain("not sent", e.Message);
        Assert.Equal(RdbRetry.BoundedJitter, e.Retry);
    }
}
