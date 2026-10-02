using System.Text;

namespace Retcd.Client;

/// <summary>
/// One stored key. <see cref="Key"/> is the key decoded as UTF-8, for display and patterns; it is lossy for keys
/// that are not valid UTF-8, where two keys can decode the same. <see cref="KeyBytes"/> is the exact key: pass it
/// to the byte overloads of Get, Put and Delete to address this record. The two never disagree: setting one,
/// in an initializer or a <c>with</c>, sets the other.
/// </summary>
public sealed record RetcdRecord(string Key, ReadOnlyMemory<byte> Value, ulong CreateRevision, ulong ModRevision)
{
    private readonly string _key = Key;
    private readonly ReadOnlyMemory<byte>? _keyBytes;

    /// <summary>The key as UTF-8 text. Setting it also sets <see cref="KeyBytes"/> to its UTF-8 bytes.</summary>
    public string Key
    {
        get => _key;
        init => (_key, _keyBytes) = (value, null);
    }

    /// <summary>
    /// The exact key bytes. A record made from a string key gets its UTF-8. Setting it also sets <see cref="Key"/>
    /// to the bytes decoded as UTF-8.
    /// </summary>
    public ReadOnlyMemory<byte> KeyBytes
    {
        get => _keyBytes ?? Encoding.UTF8.GetBytes(_key);
        init => (_key, _keyBytes) = (Encoding.UTF8.GetString(value.Span), value);
    }

    /// <summary>The value decoded as UTF-8.</summary>
    public string ValueAsString() => Encoding.UTF8.GetString(Value.Span);

    /// <summary>
    /// The key compares by its bytes, so two records for one key are equal whichever buffer holds it, and a key given
    /// as text equals the same key given as bytes. <see cref="Value"/> compares as a record member always has.
    /// </summary>
    public bool Equals(RetcdRecord? other) =>
        ReferenceEquals(this, other)
        || (other is not null && KeyBytes.Span.SequenceEqual(other.KeyBytes.Span) && Value.Equals(other.Value)
            && CreateRevision == other.CreateRevision && ModRevision == other.ModRevision);

    public override int GetHashCode() => HashCode.Combine(KeyContent.Hash(KeyBytes.Span), Value, CreateRevision, ModRevision);
}

public enum RetcdEventType
{
    Put,
    Delete,
}

/// <summary>
/// One change seen by a watch. For a <see cref="RetcdEventType.Delete"/> the value is empty.
/// <see cref="Key"/> is the UTF-8 rendering; <see cref="KeyBytes"/> is the exact key. Setting one sets the other.
/// </summary>
public sealed record RetcdEvent(RetcdEventType Type, string Key, ReadOnlyMemory<byte> Value, ulong Revision)
{
    private readonly string _key = Key;
    private readonly ReadOnlyMemory<byte>? _keyBytes;

    /// <summary>The key as UTF-8 text. Setting it also sets <see cref="KeyBytes"/> to its UTF-8 bytes.</summary>
    public string Key
    {
        get => _key;
        init => (_key, _keyBytes) = (value, null);
    }

    /// <summary>The exact key bytes. Setting it also sets <see cref="Key"/> to the bytes decoded as UTF-8.</summary>
    public ReadOnlyMemory<byte> KeyBytes
    {
        get => _keyBytes ?? Encoding.UTF8.GetBytes(_key);
        init => (_key, _keyBytes) = (Encoding.UTF8.GetString(value.Span), value);
    }

    public string ValueAsString() => Encoding.UTF8.GetString(Value.Span);

    /// <summary>The key compares by its bytes, as in <see cref="RetcdRecord"/>. <see cref="Value"/> is unchanged.</summary>
    public bool Equals(RetcdEvent? other) =>
        ReferenceEquals(this, other)
        || (other is not null && Type == other.Type && KeyBytes.Span.SequenceEqual(other.KeyBytes.Span)
            && Value.Equals(other.Value) && Revision == other.Revision);

    public override int GetHashCode() => HashCode.Combine(Type, KeyContent.Hash(KeyBytes.Span), Value, Revision);
}

internal static class KeyContent
{
    public static int Hash(ReadOnlySpan<byte> key)
    {
        var h = new HashCode();
        h.AddBytes(key);
        return h.ToHashCode();
    }
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
