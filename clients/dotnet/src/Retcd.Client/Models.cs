using System.Text;

namespace Retcd.Client;

/// <summary>One stored key.</summary>
public sealed record RetcdRecord(string Key, ReadOnlyMemory<byte> Value, ulong CreateRevision, ulong ModRevision)
{
    /// <summary>The value decoded as UTF-8.</summary>
    public string ValueAsString() => Encoding.UTF8.GetString(Value.Span);
}

public enum RetcdEventType
{
    Put,
    Delete,
}

/// <summary>One change seen by a watch. For a <see cref="RetcdEventType.Delete"/> the value is empty.</summary>
public sealed record RetcdEvent(RetcdEventType Type, string Key, ReadOnlyMemory<byte> Value, ulong Revision)
{
    public string ValueAsString() => Encoding.UTF8.GetString(Value.Span);
}

/// <summary>A folder row from <see cref="RetcdClient.ListDirsAsync"/>. <see cref="Path"/> ends with "/".</summary>
public sealed record RetcdDir(string Path, int KeyCount);

/// <summary>Keys that match the pattern, plus one row per matching sub-folder.</summary>
public sealed record RetcdDirListing(IReadOnlyList<RetcdRecord> Files, IReadOnlyList<RetcdDir> Dirs);

/// <summary>Result of <see cref="RetcdClient.PutFileAsync"/>.</summary>
public sealed record PutFileResult(string Key, long Size, string Sha256, ulong Revision, ulong MetaRevision);

/// <summary>Result of <see cref="RetcdClient.GetFileAsync"/>. <see cref="Verified"/> is false when no meta record exists.</summary>
public sealed record GetFileResult(string Key, string Path, long Size, string Sha256, ulong Revision, bool Verified);

/// <summary>What one node's health endpoint said (or why it could not be read).</summary>
public sealed record NodeHealth(
    string Endpoint,
    string HealthUrl,
    bool Reachable,
    string? Error,
    bool Ready,
    string? Role,
    ulong? NodeId,
    ulong? CurrentLeader,
    ulong? ClusterRevision,
    string? StateHashHex);

public enum PresenceState
{
    /// <summary>First beat seen.</summary>
    Up,
    /// <summary>One or more beats missed.</summary>
    Late,
    /// <summary>Silent for missedBeats intervals, or its key was deleted.</summary>
    Down,
    /// <summary>A beat arrived after Late or Down.</summary>
    Back,
}

/// <summary>A change in who is alive. <see cref="LastSeen"/> is the monitor's own clock when the last beat arrived.</summary>
public sealed record PresenceChange(string Name, PresenceState State, DateTimeOffset LastSeen);
