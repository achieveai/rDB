using Grpc.Core;

namespace Retcd.Client;

/// <summary>Base class for every error this library raises about the cluster.</summary>
public class RetcdException : Exception
{
    /// <summary>The gRPC status behind this error, when there is one.</summary>
    public StatusCode? GrpcStatus { get; }

    public RetcdException(string message, Exception? inner = null, StatusCode? grpcStatus = null)
        : base(message, inner)
    {
        GrpcStatus = grpcStatus;
    }
}

/// <summary>
/// A compare-and-set write was refused: the key is not at the revision you named.
/// Nothing was written. Read <see cref="CurrentRevision"/> and decide again.
/// </summary>
public sealed class CasConflictException : RetcdException
{
    /// <summary>The key's current mod revision. 0 when the key does not exist.</summary>
    public ulong CurrentRevision { get; }

    /// <summary>Whether the key exists right now.</summary>
    public bool Exists { get; }

    public CasConflictException(ulong currentRevision, bool exists, Exception? inner = null)
        : base(exists
            ? $"compare-and-set refused: the key is at revision {currentRevision}"
            : "compare-and-set refused: the key does not exist", inner, StatusCode.FailedPrecondition)
    {
        CurrentRevision = currentRevision;
        Exists = exists;
    }
}

/// <summary>A value (or key) is over the server limit: nothing was sent, or the server refused it.</summary>
public sealed class ValueTooLargeException : RetcdException
{
    public long Size { get; }
    public long Limit { get; }

    public ValueTooLargeException(string what, long size, long limit, Exception? inner = null)
        : base($"{what} is {size} bytes; the limit is {limit} bytes", inner, StatusCode.ResourceExhausted)
    {
        Size = size;
        Limit = limit;
    }
}

/// <summary>
/// A read result is bigger than this client holds in memory: <see cref="RetcdClient.ListDirsAsync"/> past
/// <see cref="RetcdClient.MaxListDirsBytes"/>. The client stopped partway through the walk; the server refused
/// nothing and nothing was written. Stream a result this big with <see cref="RetcdClient.ListAsync(string, int, CancellationToken)"/>.
/// </summary>
public sealed class ResultTooLargeException : RetcdException
{
    /// <summary>Bytes kept when the client stopped.</summary>
    public long Size { get; }

    /// <summary>The cap.</summary>
    public long Limit { get; }

    public ResultTooLargeException(string what, long size, long limit)
        : base($"{what} holds more than {limit} bytes (stopped at {size}); stream it with ListAsync")
    {
        Size = size;
        Limit = limit;
    }
}

/// <summary>
/// A write was sent, then the connection broke or the call timed out. It may or may not have been applied.
/// This library never retries it. Read the key to find out, or use a compare-and-set to write it again safely.
/// </summary>
public sealed class UnknownOutcomeException : RetcdException
{
    public UnknownOutcomeException(string message, Exception? inner = null, StatusCode? grpcStatus = null)
        : base(message, inner, grpcStatus)
    {
    }
}

/// <summary>No node could take the call (no leader yet, nodes down, or the server said it is unavailable). Nothing was applied.</summary>
public sealed class RetcdUnavailableException : RetcdException
{
    public RetcdUnavailableException(string message, Exception? inner = null, StatusCode? grpcStatus = null)
        : base(message, inner, grpcStatus)
    {
    }
}

/// <summary>A watch asked for a revision that has been compacted away. List again, then watch from the list's revision.</summary>
public sealed class RevisionCompactedException : RetcdException
{
    /// <summary>The oldest revision still available (0 if the server did not say).</summary>
    public ulong MinRevision { get; }

    public RevisionCompactedException(ulong minRevision, Exception? inner = null)
        : base($"that revision was compacted away; the oldest available is {minRevision}", inner, StatusCode.OutOfRange)
    {
        MinRevision = minRevision;
    }
}

/// <summary>GetFileAsync asked for a key that is not there.</summary>
public sealed class RetcdNotFoundException : RetcdException
{
    public string Key { get; }

    public RetcdNotFoundException(string key)
        : base($"not found: {key}", null, StatusCode.NotFound)
    {
        Key = key;
    }
}

/// <summary>The stored bytes do not match the sha256 / size in the <c>meta/&lt;key&gt;</c> record. The file was not written.</summary>
public sealed class ChecksumMismatchException : RetcdException
{
    public ChecksumMismatchException(string message) : base(message)
    {
    }
}
