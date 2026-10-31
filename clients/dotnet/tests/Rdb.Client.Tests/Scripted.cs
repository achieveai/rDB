using Google.Protobuf;
using Grpc.Core;
using Pb = Rdb.V1;

namespace Rdb.Client.Tests;

/// <summary>
/// A gateway that answers from a script: each call takes the next entry, a reply or an
/// <see cref="RpcException"/>. A call past the end of the script fails the test loudly.
/// </summary>
internal sealed class ScriptedInvoker : CallInvoker
{
    private readonly Queue<object> _script = new();

    public List<string> Calls { get; } = new();

    public ScriptedInvoker Then(object replyOrError)
    {
        _script.Enqueue(replyOrError);
        return this;
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        Calls.Add(method.Name);
        if (_script.Count == 0) throw new InvalidOperationException($"unscripted call {method.Name} (call {Calls.Count})");
        var next = _script.Dequeue();
        var answer = next is Exception ex ? Task.FromException<TResponse>(ex) : Task.FromResult((TResponse)next);
        return new AsyncUnaryCall<TResponse>(answer, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();
}

/// <summary>What the gateway sends: replies, and refusals carrying an RdbError as rdb-server writes them.</summary>
internal static class Wire
{
    public static Pb.WriteReply Written(ulong requestId = 11, ulong version = 5) =>
        new() { RequestId = requestId, Generation = 2, Version = version, Outcome = Pb.Outcome.Published };

    public static Pb.GetReply Found(string value, ulong version = 5) =>
        new() { Found = true, Version = version, Value = ByteString.CopyFromUtf8(value), Generation = 2 };

    /// <summary>A refusal the server decided, in the rich status trailer.</summary>
    public static RpcException Refusal(
        StatusCode code, Pb.ErrorKind kind, string kindName, Pb.RetryRule retry, bool noMutation, ulong? requestId = null)
    {
        var detail = new Pb.RdbError { Kind = kind, KindName = kindName, Retry = retry, NoMutation = noMutation };
        if (requestId is { } id) detail.RequestId = id;
        var status = new Pb.RpcStatus { Code = (int)code, Message = $"{kindName} (detail; next: step)" };
        status.Details.Add(new Pb.AnyDetail { TypeUrl = RdbErrors.RdbErrorTypeUrl, Value = detail.ToByteString() });
        var trailers = new Metadata { { RdbErrors.DetailsTrailer, status.ToByteArray() } };
        return new RpcException(new Status(code, status.Message), trailers);
    }

    /// <summary>An error the transport made: no RdbError, the server did not decide.</summary>
    public static RpcException Lost(StatusCode code = StatusCode.Unavailable, string detail = "") =>
        new(new Status(code, detail));
}
