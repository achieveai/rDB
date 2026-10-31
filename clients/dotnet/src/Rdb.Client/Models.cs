using System.Text;

namespace Rdb.Client;

/// <summary>Settings for <see cref="RdbClient.Create"/>.</summary>
public sealed class RdbClientOptions
{
    /// <summary>The gateway: "host:port" or "http://host:port". The dev script prints it as RDB_ENDPOINT.</summary>
    public string Endpoint { get; set; } = "http://127.0.0.1:19190";

    /// <summary>
    /// Time budget for one call, retries included. Default 30 s. One attempt can take the gateway up to about
    /// 12 s for a put and the transaction deadline plus about 9 s for a transaction, so a retry is started only when
    /// a whole attempt still fits. A budget shorter than one attempt still makes one attempt, cut at the budget: a
    /// write cut that way is <see cref="RdbUnknownOutcomeException"/> with no request id.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>What the server says about readiness.</summary>
public sealed record RdbReady(bool Ready, string Detail, uint ClientId, TimeSpan PutTimeout, TimeSpan ReadTimeout);

/// <summary>How a write landed.</summary>
public enum RdbOutcome
{
    /// <summary>Not said (an older or newer server).</summary>
    Unspecified = 0,
    /// <summary>Applied and published.</summary>
    Published = 1,
    /// <summary>Found in retained history after a recovery.</summary>
    RecoveredApplied = 2,
}

/// <summary>A published write. Keep <see cref="RequestId"/> and <see cref="Generation"/> to ask <see cref="RdbClient.StatusAsync"/>.</summary>
public sealed record RdbWrite(ulong Version, ulong RequestId, ulong Generation, RdbOutcome Outcome);

/// <summary>An object read back.</summary>
public sealed record RdbRecord(ulong Version, ReadOnlyMemory<byte> Value, ulong Generation, bool RecoveryMode)
{
    /// <summary>The value as UTF-8 text.</summary>
    public string ValueAsString() => Encoding.UTF8.GetString(Value.Span);
}

/// <summary>One put of a transaction.</summary>
public sealed record RdbTxnPut(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value, ulong? IfVersion = null)
{
    /// <summary>A put with UTF-8 key and value.</summary>
    public RdbTxnPut(string key, string value, ulong? ifVersion = null)
        : this(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ifVersion)
    {
    }
}

/// <summary>Did a write land? The answer <see cref="RdbClient.StatusAsync"/> gives.</summary>
public enum RdbLanded
{
    /// <summary>It landed: <see cref="RdbStatus.Version"/> says where.</summary>
    Yes,
    /// <summary>Not known yet. Not proof it did not run: ask again.</summary>
    NotKnown,
    /// <summary>Too old to say. Not proof it did not run: read the object to see its value.</summary>
    ReadTheObject,
}

/// <summary>The answer to a status query. Version, Generation and Outcome are set only when <see cref="Landed"/> is Yes.</summary>
public sealed record RdbStatus(RdbLanded Landed, ulong Version, ulong Generation, RdbOutcome Outcome);
