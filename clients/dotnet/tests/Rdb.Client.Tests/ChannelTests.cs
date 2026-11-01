using System.Buffers.Binary;
using System.Net;
using Google.Protobuf;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pb = Rdb.V1;

namespace Rdb.Client.Tests;

/// <summary>
/// One client's calls share one real <see cref="Grpc.Net.Client.GrpcChannel"/>. A fault on one call must not end the
/// others (review F-001): disposing a channel cancels every call on it.
/// </summary>
public class ChannelTests
{
    [Fact]
    public async Task A_failing_read_does_not_cancel_a_slow_write_on_the_same_client()
    {
        await using var gateway = await FakeGateway.StartAsync();
        using var client = RdbClient.Create(new RdbClientOptions { Endpoint = gateway.Address });

        var put = client.PutAsync("k", "v");
        await gateway.PutArrived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The first Get fails in transport (UNAVAILABLE, no RdbError); the client reads again on a fresh channel.
        var record = await client.GetAsync("k").WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("v", record!.ValueAsString());
        Assert.Equal(2, gateway.Gets);

        gateway.ReleasePut.SetResult();
        var written = await put.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(41UL, written.RequestId);
    }

    // F-015: past the gateway's 64 streams on one connection, a write must not end "unknown" unless it was sent.
    // 70 writes on one client against a gateway that holds every Put it receives; each attempt is 1.5 s.
    [Fact]
    public async Task A_write_queued_past_the_stream_limit_is_never_reported_unknown_unless_it_was_sent()
    {
        await using var gateway = await FakeGateway.StartAsync();
        using var client = RdbClient.Create(new RdbClientOptions { Endpoint = gateway.Address, Timeout = TimeSpan.FromSeconds(2) });
        client.UseServerTimeouts(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500));
        client.Slack = TimeSpan.FromMilliseconds(500);
        var writes = Enumerable.Range(0, 70).Select(i => client.PutAsync($"k{i}", "v")).ToArray();
        var ends = await Task.WhenAll(writes.Select(async w =>
        {
            try { await w; return "OK"; }
            catch (RdbUnknownOutcomeException) { return "UNKNOWN"; }
            catch (RdbException e) { return e.KindName; }
        })).WaitAsync(TimeSpan.FromSeconds(20));
        var unknown = ends.Count(e => e == "UNKNOWN");
        Assert.True(unknown <= gateway.Puts,
            $"{unknown} writes ended unknown but only {gateway.Puts} reached the gateway: " +
            string.Join(", ", ends.GroupBy(e => e).Select(g => $"{g.Key}={g.Count()}")));
    }
}

/// <summary>
/// A gateway on Kestrel over HTTP/2 with gRPC framing written by hand: Put waits until released, the first Get fails
/// with a transport-style UNAVAILABLE, later Gets find the value.
/// </summary>
internal sealed class FakeGateway : IAsyncDisposable
{
    private readonly WebApplication _app;
    private int _gets;
    private int _puts;

    private FakeGateway(WebApplication app, string address)
    {
        _app = app;
        Address = address;
    }

    public string Address { get; }

    public TaskCompletionSource PutArrived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource ReleasePut { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Gets => Volatile.Read(ref _gets);

    /// <summary>Puts that reached the gateway.</summary>
    public int Puts => Volatile.Read(ref _puts);

    public static async Task<FakeGateway> StartAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k =>
        {
            // As the gateway: 64 streams per connection (rdb-server MAX_IN_FLIGHT).
            k.Limits.Http2.MaxStreamsPerConnection = 64;
            k.Listen(IPAddress.Loopback, 0, l => l.Protocols = HttpProtocols.Http2);
        });
        var app = builder.Build();
        FakeGateway? self = null;
        app.MapPost("/rdb.v1.Rdb/Put", async ctx =>
        {
            await ctx.Request.Body.CopyToAsync(Stream.Null);
            Interlocked.Increment(ref self!._puts);
            self.PutArrived.TrySetResult();
            await self.ReleasePut.Task.WaitAsync(ctx.RequestAborted);
            await Reply(ctx, new Pb.WriteReply { RequestId = 41, Generation = 2, Version = 7, Outcome = Pb.Outcome.Published });
        });
        app.MapPost("/rdb.v1.Rdb/Get", async ctx =>
        {
            await ctx.Request.Body.CopyToAsync(Stream.Null);
            if (Interlocked.Increment(ref self!._gets) == 1)
            {
                ctx.Response.ContentType = "application/grpc";
                await ctx.Response.StartAsync();
                ctx.Response.AppendTrailer("grpc-status", "14");
                ctx.Response.AppendTrailer("grpc-message", "injected transport fault");
                return;
            }
            await Reply(ctx, new Pb.GetReply { Found = true, Version = 7, Value = ByteString.CopyFromUtf8("v"), Generation = 2 });
        });
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        self = new FakeGateway(app, address);
        return self;
    }

    private static async Task Reply(HttpContext ctx, IMessage message)
    {
        var body = message.ToByteArray();
        var frame = new byte[5 + body.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(1), (uint)body.Length);
        body.CopyTo(frame, 5);
        ctx.Response.ContentType = "application/grpc";
        await ctx.Response.Body.WriteAsync(frame);
        ctx.Response.AppendTrailer("grpc-status", "0");
    }

    public async ValueTask DisposeAsync()
    {
        ReleasePut.TrySetResult();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
