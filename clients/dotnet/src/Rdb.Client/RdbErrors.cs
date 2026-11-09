using Grpc.Core;
using Pb = Rdb.V1;

namespace Rdb.Client;

/// <summary>Reading the server's decision out of a gRPC error, and the one retry rule.</summary>
internal static class RdbErrors
{
    public const string DetailsTrailer = "grpc-status-details-bin";
    public const string RdbErrorTypeUrl = "type.googleapis.com/rdb.v1.RdbError";

    /// <summary>The RdbError in the rich status details, or null when the server did not decide (the transport made the error).</summary>
    public static Pb.RdbError? Detail(RpcException ex)
    {
        var bytes = ex.Trailers.GetValueBytes(DetailsTrailer);
        if (bytes is null) return null;
        try
        {
            var status = Pb.RpcStatus.Parser.ParseFrom(bytes);
            foreach (var d in status.Details)
            {
                if (d.TypeUrl == RdbErrorTypeUrl) return Pb.RdbError.Parser.ParseFrom(d.Value);
            }
        }
        catch (Google.Protobuf.InvalidProtocolBufferException)
        {
            // Not a status this server wrote: the server did not decide.
        }
        return null;
    }

    public static RdbErrorKind Kind(Pb.RdbError d) =>
        Enum.IsDefined(typeof(RdbErrorKind), (int)d.Kind) && d.Kind != Pb.ErrorKind.Unspecified
            ? (RdbErrorKind)(int)d.Kind
            : RdbErrorKind.Unrecognized;

    public static RdbRetry Retry(Pb.RdbError d) =>
        Enum.IsDefined(typeof(RdbRetry), (int)d.Retry) ? (RdbRetry)(int)d.Retry : RdbRetry.Unrecognized;

    /// <summary>The rules under which a refused call may be sent again: the server says so and the kind is known.</summary>
    public static bool RetryableRule(RdbErrorKind kind, RdbRetry retry) =>
        kind != RdbErrorKind.Unrecognized
        && retry is RdbRetry.RefreshRoute or RdbRetry.RetryAfterRecovery or RdbRetry.BoundedJitter;

    /// <summary>
    /// The one retry predicate. A write is sent again (as a new write) only when the server decided, the call
    /// provably changed nothing, and the rule allows it. A read changes nothing, so the rule alone decides.
    /// </summary>
    public static bool MayRetry(Pb.RdbError? d, bool isWrite)
    {
        if (d is null) return false;
        if (!RetryableRule(Kind(d), Retry(d))) return false;
        return !isWrite || d.NoMutation;
    }

    public static TimeSpan? RetryAfter(Pb.RdbError? d) =>
        d is { HasRetryAfterMs: true } ? TimeSpan.FromMilliseconds(d.RetryAfterMs) : null;

    /// <summary>A server-decided error as the exception an app branches on.</summary>
    public static RdbException Map(RpcException ex, Pb.RdbError d, bool isWrite)
    {
        var kind = Kind(d);
        var retry = Retry(d);
        ulong? id = d.HasRequestId ? d.RequestId : null;
        ulong? gen = d.HasGeneration ? d.Generation : null;
        var message = ex.Status.Detail;
        if (kind == RdbErrorKind.ConditionFailed) return new RdbConditionFailedException(message, retry, d.NoMutation, id, gen, ex);
        if (kind == RdbErrorKind.UnknownOutcome || (isWrite && !d.NoMutation))
        {
            return new RdbUnknownOutcomeException(message, kind, d.KindName, retry, id, gen, RetryAfter(d), true, ex);
        }
        return new RdbException(message, kind, d.KindName, retry, d.NoMutation, id, gen, RetryAfter(d), true, ex);
    }
}
