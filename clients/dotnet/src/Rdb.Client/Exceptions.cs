namespace Rdb.Client;

/// <summary>The error kinds of rDB (spec §5.4). Numbered as on the wire.</summary>
public enum RdbErrorKind
{
    /// <summary>A kind this client does not know (a newer server). See <see cref="RdbException.KindName"/>. Never retried.</summary>
    Unrecognized = 0,
    NotPrimary = 1,
    RouteChanged = 2,
    LeaseExpired = 3,
    RecoveryReadOnly = 4,
    ProtectionPaused = 5,
    ConditionFailed = 6,
    CrossAffinity = 7,
    InvalidArgument = 8,
    Overloaded = 9,
    DeadlineBeforeAdmission = 10,
    UnknownOutcome = 11,
    GenerationChanged = 12,
    RequestIdReuse = 13,
    StaleContinuation = 14,
    CorruptHistory = 15,
    IncompatibleVersion = 16,
    StatusExpired = 17,
    DivergenceRequiresOperator = 18,
    Unavailable = 19,
    UploadChunkConflict = 20,
    CorruptBlob = 21,
    /// <summary>A durability barrier ended without confirming the position: a regular copy had not proved it (the message names the copies still owed), or every copy proved it but the owner's grant stopped admitting before it could confirm (the message names no copy; read back). Both cases are owner answers. A third producer is not an owner: when no owner answer arrives in time, the embedded Rust client's <c>Db::wait_durable</c> reports this code itself, and its message says which copies are owed is not known. This .NET client has no barrier call, so it never produces this code; it only reads it. Says nothing about the write.</summary>
    DurabilityUnconfirmed = 22,
}

/// <summary>What the caller may do after an error. Numbered as on the wire.</summary>
public enum RdbRetry
{
    /// <summary>Not said, or a rule this client does not know. Never retried.</summary>
    Unrecognized = 0,
    RefreshRoute = 1,
    RetryAfterRecovery = 2,
    Definitive = 3,
    BoundedJitter = 4,
    QueryStatus = 5,
    Reconcile = 6,
    Quarantine = 7,
    NotWired = 8,
}

/// <summary>
/// An rDB call failed. <see cref="Exception.Message"/> carries the server's words and next step.
/// <see cref="FromServer"/> false means the server never decided: the call was not sent (it could not connect),
/// or for a read the transport lost it.
/// </summary>
public class RdbException : Exception
{
    public RdbErrorKind Kind { get; }

    /// <summary>The kind's wire name, such as CONDITION_FAILED. Kept even when <see cref="Kind"/> is Unrecognized.</summary>
    public string KindName { get; }

    public RdbRetry Retry { get; }

    /// <summary>This call provably changed nothing.</summary>
    public bool NoMutation { get; }

    /// <summary>The write's request id, when one was sent. Ask <see cref="RdbClient.StatusAsync"/> with it.</summary>
    public ulong? RequestId { get; }

    /// <summary>The generation to pass to <see cref="RdbClient.StatusAsync"/> with <see cref="RequestId"/>.</summary>
    public ulong? Generation { get; }

    /// <summary>The soonest a retry can be admitted, when the server knows it.</summary>
    public TimeSpan? RetryAfter { get; }

    /// <summary>Whether the server decided this error (it carried an RdbError).</summary>
    public bool FromServer { get; }

    /// <summary>How many attempts the call made.</summary>
    public int Attempts { get; internal set; }

    /// <summary>How long the call took, retries included.</summary>
    public TimeSpan Elapsed { get; internal set; }

    public RdbException(
        string message, RdbErrorKind kind, string kindName, RdbRetry retry, bool noMutation,
        ulong? requestId, ulong? generation, TimeSpan? retryAfter, bool fromServer, Exception? inner = null)
        : base(message, inner)
    {
        Kind = kind;
        KindName = kindName;
        Retry = retry;
        NoMutation = noMutation;
        RequestId = requestId;
        Generation = generation;
        RetryAfter = retryAfter;
        FromServer = fromServer;
    }

    /// <summary>Every field, one line: what the sample's --verbose prints.</summary>
    public string Describe() =>
        $"kind={KindName} retry={Retry} no_mutation={NoMutation.ToString().ToLowerInvariant()} " +
        $"request_id={RequestId?.ToString() ?? "-"} generation={Generation?.ToString() ?? "-"} " +
        $"retry_after_ms={(RetryAfter is { } r ? ((long)r.TotalMilliseconds).ToString() : "-")} " +
        $"from_server={FromServer.ToString().ToLowerInvariant()} attempts={Attempts} elapsed_ms={(long)Elapsed.TotalMilliseconds}";
}

/// <summary>A conditional write was refused: an object was not at the version named. Nothing was written. Read and decide again.</summary>
public sealed class RdbConditionFailedException : RdbException
{
    public RdbConditionFailedException(
        string message, RdbRetry retry, bool noMutation, ulong? requestId, ulong? generation, Exception? inner = null)
        : base(message, RdbErrorKind.ConditionFailed, "CONDITION_FAILED", retry, noMutation, requestId, generation, null, true, inner)
    {
    }
}

/// <summary>
/// A write may or may not have applied. It is never sent again for you. When <see cref="RdbException.RequestId"/> is
/// set, ask <see cref="RdbClient.StatusAsync"/> with it and <see cref="RdbException.Generation"/>; when it is not (the
/// answer was lost on the way), read the object. <see cref="RdbException.Kind"/> is the server's kind, or
/// UnknownOutcome when the transport lost the answer.
/// </summary>
public sealed class RdbUnknownOutcomeException : RdbException
{
    public RdbUnknownOutcomeException(
        string message, RdbErrorKind kind, string kindName, RdbRetry retry, ulong? requestId, ulong? generation,
        TimeSpan? retryAfter, bool fromServer, Exception? inner = null)
        : base(message, kind, kindName, retry, false, requestId, generation, retryAfter, fromServer, inner)
    {
    }
}
