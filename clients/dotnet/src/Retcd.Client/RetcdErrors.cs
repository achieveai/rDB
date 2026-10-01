using System.Globalization;
using System.Net.Sockets;
using Grpc.Core;

namespace Retcd.Client;

/// <summary>
/// Reads gRPC failures the way ADR-0015 says to. The server stamps every refusal it decides with
/// <c>retcd-outcome: rejected</c>. A status without that stamp came from the transport, and for a
/// write it means "may or may not have been applied".
/// </summary>
internal static class RetcdErrors
{
    public const string HeaderOutcome = "retcd-outcome";
    public const string HeaderLeaderEndpoint = "retcd-leader-endpoint";
    public const string HeaderConflictExists = "retcd-conflict-exists";
    public const string HeaderConflictModRevision = "retcd-conflict-mod-revision";
    public const string HeaderMinRevision = "retcd-min-revision";
    public const string HeaderResumable = "retcd-resumable";
    public const string HeaderReason = "retcd-reason";

    public static string? Meta(RpcException ex, string key)
    {
        foreach (var e in ex.Trailers)
        {
            if (string.Equals(e.Key, key, StringComparison.OrdinalIgnoreCase) && !e.IsBinary) return e.Value;
        }
        return null;
    }

    public static bool IsServerRejection(RpcException ex) => Meta(ex, HeaderOutcome) == "rejected";

    public static string? LeaderHint(RpcException ex)
    {
        var hint = Meta(ex, HeaderLeaderEndpoint);
        return string.IsNullOrWhiteSpace(hint) ? null : hint;
    }

    public static bool IsConflict(RpcException ex, out ulong currentRevision, out bool exists)
    {
        currentRevision = 0;
        exists = false;
        if (ex.StatusCode != StatusCode.FailedPrecondition) return false;
        var rev = Meta(ex, HeaderConflictModRevision);
        if (rev is null || !ulong.TryParse(rev, NumberStyles.None, CultureInfo.InvariantCulture, out currentRevision)) return false;
        exists = Meta(ex, HeaderConflictExists) == "true";
        return true;
    }

    /// <summary>The page-token refusal reason (mac, expired, evicted, node, ...), if this is one.</summary>
    public static string? PageTokenReason(RpcException ex) =>
        ex.StatusCode == StatusCode.FailedPrecondition ? Meta(ex, HeaderReason) : null;

    /// <summary>A follower answering "not the leader": FAILED_PRECONDITION that is neither a conflict nor a page-token refusal.</summary>
    public static bool IsNotLeader(RpcException ex) =>
        ex.StatusCode == StatusCode.FailedPrecondition
        && !IsConflict(ex, out _, out _)
        && PageTokenReason(ex) is null;

    /// <summary>
    /// "No leader yet": UNAVAILABLE that the server stamped and gave no reason. It was refused before the
    /// Raft log, so nothing was applied and a resend cannot duplicate it, even for a write. With a reason
    /// (feature_not_activated) it is final; without the stamp it came from the transport and a write's
    /// outcome is unknown.
    /// </summary>
    public static bool IsNoLeaderYet(RpcException ex) =>
        ex.StatusCode == StatusCode.Unavailable && IsServerRejection(ex) && Meta(ex, HeaderReason) is null;

    /// <summary>A List cursor minted by another node. The refusal names a better node; follow it with the same token.</summary>
    public static bool IsForeignPageToken(RpcException ex) =>
        PageTokenReason(ex) == "node" && LeaderHint(ex) is not null;

    /// <summary>
    /// The connection could not be made, so nothing was sent. A reset on an open connection is not this.
    /// </summary>
    public static bool IsConnectFailure(RpcException ex)
    {
        if (ex.StatusCode != StatusCode.Unavailable || IsServerRejection(ex)) return false;
        for (Exception? e = ex.Status.DebugException ?? ex.InnerException; e is not null; e = e.InnerException)
        {
            if (e is SocketException se && IsConnectSocketError(se.SocketErrorCode)) return true;
        }
        var detail = ex.Status.Detail ?? "";
        return detail.Contains("Error connecting to subchannel", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsConnectSocketError(SocketError e) => e is
        SocketError.ConnectionRefused or SocketError.HostNotFound or SocketError.HostUnreachable
        or SocketError.NetworkUnreachable or SocketError.AddressNotAvailable or SocketError.TryAgain
        or SocketError.NoData;

    public static bool Resumable(RpcException ex) => Meta(ex, HeaderResumable) == "true";

    public static ulong MinRevision(RpcException ex) =>
        ulong.TryParse(Meta(ex, HeaderMinRevision), NumberStyles.None, CultureInfo.InvariantCulture, out var v) ? v : 0;

    /// <summary>
    /// Turns a failure that is not retryable into the right exception.
    /// <paramref name="valueSize"/> is the size of the value being written, for the too-large message.
    /// </summary>
    public static RetcdException Map(RpcException ex, string op, bool isWrite, long valueSize = 0)
    {
        var detail = string.IsNullOrEmpty(ex.Status.Detail) ? ex.StatusCode.ToString() : ex.Status.Detail;
        var rejected = IsServerRejection(ex);

        if (IsConflict(ex, out var rev, out var exists)) return new CasConflictException(rev, exists, ex);

        switch (ex.StatusCode)
        {
            case StatusCode.DeadlineExceeded:
                return isWrite
                    ? new UnknownOutcomeException($"{op}: timed out. The write may or may not have been applied; read the key to find out.", ex, ex.StatusCode)
                    : new RetcdUnavailableException($"{op}: timed out waiting for the cluster.", ex, ex.StatusCode);
            case StatusCode.OutOfRange:
                return new RevisionCompactedException(MinRevision(ex), ex);
            case StatusCode.ResourceExhausted when isWrite && rejected && valueSize > 0:
                return new ValueTooLargeException("value", valueSize, Limits.MaxValueBytes, ex);
            case StatusCode.FailedPrecondition when PageTokenReason(ex) is { } reason:
                return new RetcdException($"{op}: the list cursor was refused ({reason}). Start the list again.", ex, ex.StatusCode);
            case StatusCode.FailedPrecondition:
                return new RetcdUnavailableException($"{op}: not the leader and no leader hint. Nothing was applied.", ex, ex.StatusCode);
        }

        if (isWrite && !rejected && IsConnectFailure(ex))
        {
            return new RetcdUnavailableException($"{op}: cannot connect. Nothing was sent.", ex, ex.StatusCode);
        }

        // A write whose status the server did not stamp may have been applied (ADR-0015).
        if (isWrite && !rejected && ex.StatusCode is StatusCode.Unavailable or StatusCode.Unknown or StatusCode.Internal
                or StatusCode.Aborted or StatusCode.DataLoss or StatusCode.Cancelled)
        {
            return new UnknownOutcomeException($"{op}: the connection failed after the write was sent ({detail}). It may or may not have been applied; read the key to find out.", ex, ex.StatusCode);
        }

        if (ex.StatusCode == StatusCode.Unavailable)
        {
            return new RetcdUnavailableException($"{op}: unavailable ({detail})", ex, ex.StatusCode);
        }

        var prefix = ex.StatusCode == StatusCode.InvalidArgument ? "bad request" : ex.StatusCode.ToString();
        return new RetcdException($"{op}: {prefix}: {detail}", ex, ex.StatusCode);
    }
}

/// <summary>Server limits (crates/config-core/src/limits.rs).</summary>
internal static class Limits
{
    public const int MaxValueBytes = 1024 * 1024;
    public const int MaxKeyBytes = 1024;
    public const int MaxListItems = 1000;
}
