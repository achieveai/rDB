using System.Net.Sockets;
using Grpc.Core;
using Retcd.Client;

namespace Retcd.Client.Tests;

public class ErrorMappingTests
{
    private static RpcException Rpc(StatusCode code, string detail = "x", bool rejected = true, Exception? debug = null, params (string, string)[] trailers)
    {
        var md = new Metadata();
        if (rejected) md.Add("retcd-outcome", "rejected");
        foreach (var (k, v) in trailers) md.Add(k, v);
        return new RpcException(new Status(code, detail, debug), md);
    }

    [Fact]
    public void Conflict_trailers_become_CasConflictException()
    {
        var ex = Rpc(StatusCode.FailedPrecondition, trailers: new[] { ("retcd-conflict-mod-revision", "42"), ("retcd-conflict-exists", "true") });
        var mapped = Assert.IsType<CasConflictException>(RetcdErrors.Map(ex, "put", isWrite: true));
        Assert.Equal(42UL, mapped.CurrentRevision);
        Assert.True(mapped.Exists);
        Assert.False(RetcdErrors.IsNotLeader(ex));
    }

    [Fact]
    public void Conflict_on_missing_key_says_it_does_not_exist()
    {
        var ex = Rpc(StatusCode.FailedPrecondition, trailers: new[] { ("retcd-conflict-mod-revision", "0"), ("retcd-conflict-exists", "false") });
        var mapped = Assert.IsType<CasConflictException>(RetcdErrors.Map(ex, "put", true));
        Assert.False(mapped.Exists);
        Assert.Equal(0UL, mapped.CurrentRevision);
    }

    [Fact]
    public void Follower_answer_is_not_leader_and_carries_the_hint()
    {
        var ex = Rpc(StatusCode.FailedPrecondition, "not the leader", trailers: new[] { ("retcd-leader-endpoint", "127.0.0.1:17612") });
        Assert.True(RetcdErrors.IsNotLeader(ex));
        Assert.Equal("127.0.0.1:17612", RetcdErrors.LeaderHint(ex));
        Assert.False(RetcdErrors.IsForeignPageToken(ex));
    }

    [Fact]
    public void Page_token_refusal_is_not_a_leader_chase_unless_reason_is_node()
    {
        var expired = Rpc(StatusCode.FailedPrecondition, trailers: new[] { ("retcd-reason", "expired"), ("retcd-leader-endpoint", "127.0.0.1:1") });
        Assert.False(RetcdErrors.IsNotLeader(expired));
        Assert.False(RetcdErrors.IsForeignPageToken(expired));
        var mapped = RetcdErrors.Map(expired, "list", false);
        Assert.IsType<RetcdException>(mapped);
        Assert.Contains("expired", mapped.Message);

        var node = Rpc(StatusCode.FailedPrecondition, trailers: new[] { ("retcd-reason", "node"), ("retcd-leader-endpoint", "127.0.0.1:1") });
        Assert.True(RetcdErrors.IsForeignPageToken(node));
    }

    [Fact]
    public void No_leader_yet_is_only_a_stamped_unavailable_with_no_reason()
    {
        Assert.True(RetcdErrors.IsNoLeaderYet(Rpc(StatusCode.Unavailable, "no leader")));
        Assert.False(RetcdErrors.IsNoLeaderYet(Rpc(StatusCode.Unavailable, trailers: new[] { ("retcd-reason", "feature_not_activated") })));
        Assert.False(RetcdErrors.IsNoLeaderYet(Rpc(StatusCode.Unavailable, rejected: false)), "unstamped: from the transport");
        Assert.False(RetcdErrors.IsNoLeaderYet(Rpc(StatusCode.FailedPrecondition)));
    }

    [Fact]
    public void Timed_out_write_is_unknown_outcome_but_timed_out_read_is_just_unavailable()
    {
        var ex = Rpc(StatusCode.DeadlineExceeded, rejected: false);
        Assert.IsType<UnknownOutcomeException>(RetcdErrors.Map(ex, "put", isWrite: true));
        Assert.IsType<RetcdUnavailableException>(RetcdErrors.Map(ex, "get", isWrite: false));
    }

    [Fact]
    public void Connection_lost_after_send_is_unknown_outcome_for_a_write()
    {
        var reset = new HttpRequestException("reset", new SocketException((int)SocketError.ConnectionReset));
        var ex = Rpc(StatusCode.Unavailable, "Error starting gRPC call", rejected: false, debug: reset);
        Assert.False(RetcdErrors.IsConnectFailure(ex));
        Assert.IsType<UnknownOutcomeException>(RetcdErrors.Map(ex, "put", isWrite: true));
        Assert.IsType<RetcdUnavailableException>(RetcdErrors.Map(ex, "get", isWrite: false));
    }

    [Fact]
    public void Refused_connection_means_nothing_was_sent()
    {
        var refused = new HttpRequestException("refused", new SocketException((int)SocketError.ConnectionRefused));
        var ex = Rpc(StatusCode.Unavailable, "Error connecting to subchannel.", rejected: false, debug: refused);
        Assert.True(RetcdErrors.IsConnectFailure(ex));
        Assert.IsType<RetcdUnavailableException>(RetcdErrors.Map(ex, "put", isWrite: true));
    }

    [Fact]
    public void Server_stamped_unavailable_is_known_not_applied()
    {
        var ex = Rpc(StatusCode.Unavailable, "feature not active", rejected: true);
        Assert.IsType<RetcdUnavailableException>(RetcdErrors.Map(ex, "put", isWrite: true));
    }

    [Fact]
    public void Compacted_watch_carries_the_oldest_revision()
    {
        var ex = Rpc(StatusCode.OutOfRange, trailers: new[] { ("retcd-min-revision", "77") });
        var mapped = Assert.IsType<RevisionCompactedException>(RetcdErrors.Map(ex, "watch", false));
        Assert.Equal(77UL, mapped.MinRevision);
    }

    [Fact]
    public void Refused_big_write_becomes_ValueTooLarge()
    {
        var ex = Rpc(StatusCode.ResourceExhausted, "too big");
        var mapped = Assert.IsType<ValueTooLargeException>(RetcdErrors.Map(ex, "put", true, valueSize: 2_000_000));
        Assert.Equal(2_000_000, mapped.Size);
        Assert.Equal(1024 * 1024, mapped.Limit);
    }

    [Fact]
    public void Resumable_header_is_read()
    {
        Assert.True(RetcdErrors.Resumable(Rpc(StatusCode.ResourceExhausted, trailers: new[] { ("retcd-resumable", "true") })));
        Assert.False(RetcdErrors.Resumable(Rpc(StatusCode.ResourceExhausted)));
    }

    [Fact]
    public void Bad_request_keeps_the_server_detail()
    {
        var mapped = RetcdErrors.Map(Rpc(StatusCode.InvalidArgument, "key too long"), "put", true);
        Assert.IsType<RetcdException>(mapped);
        Assert.Contains("key too long", mapped.Message);
    }

    [Theory]
    [InlineData("127.0.0.1:17602", "http://127.0.0.1:17602")]
    [InlineData("  localhost:5000 ", "http://localhost:5000")]
    [InlineData("http://h:1/", "http://h:1")]
    [InlineData("https://h:2", "https://h:2")]
    public void Endpoints_are_normalised(string input, string expected)
    {
        Assert.Equal(expected, Transport.Normalize(input, "http"));
    }

    [Fact]
    public void Bad_endpoint_is_refused_up_front()
    {
        Assert.Throws<ArgumentException>(() => Transport.Normalize("nonsense", "http"));
        Assert.Throws<ArgumentException>(() => RetcdClient.Create(new RetcdClientOptions()));
    }

    [Fact]
    public void Health_url_is_client_port_plus_two()
    {
        Assert.Equal("http://127.0.0.1:17604/health", RetcdClient.DeriveHealthUrl("http://127.0.0.1:17602"));
    }
}
