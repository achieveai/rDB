using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace Retcd.Client;

/// <summary>
/// "Who is alive", without leases. Each process writes <c>presence/&lt;name&gt;</c> every interval:
/// <c>{"name","pid","seq","sent_at"}</c> (sent_at is ISO-8601 UTC). A monitor watches <c>presence/</c>
/// and judges silence by its own clock, so clock skew between machines does not matter.
/// </summary>
public static class RetcdPresence
{
    public const string KeyPrefix = "presence/";

    public static string KeyFor(string name) => KeyPrefix + name;

    /// <summary>The exact JSON a heartbeat writes. Same format as the Node library and presence.mjs.</summary>
    public static string BuildBeat(string name, int pid, long seq, DateTimeOffset sentAt)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            w.WriteString("name", name);
            w.WriteNumber("pid", pid);
            w.WriteNumber("seq", seq);
            w.WriteString("sent_at", sentAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", System.Globalization.CultureInfo.InvariantCulture));
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    /// <summary>
    /// Start writing a beat now and then every <paramref name="interval"/>, until disposed.
    /// Dispose deletes the key (a clean goodbye, which monitors report as Down at once) unless
    /// <paramref name="removeOnDispose"/> is false (what a crash looks like).
    /// </summary>
    public static PresenceHeartbeat StartHeartbeat(RetcdClient client, string name, TimeSpan interval, bool removeOnDispose = true)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name is empty", nameof(name));
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        return new PresenceHeartbeat(client, name, interval, removeOnDispose);
    }

    /// <summary>
    /// Report changes in who is alive. Everyone already under <c>presence/</c> is reported Up first.
    /// Then: Late after 1.5 intervals of silence, Down after <paramref name="missedBeats"/> intervals
    /// (or at once if the key is deleted), Back when a beat comes after Late or Down.
    /// A name that was already dead when the monitor started is first reported Up, then Down after
    /// <c>missedBeats × interval</c>. Use the same <paramref name="interval"/> the clients use.
    /// </summary>
    public static async IAsyncEnumerable<PresenceChange> WatchAsync(
        RetcdClient client, TimeSpan interval, int missedBeats = 3,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        if (missedBeats < 2) throw new ArgumentOutOfRangeException(nameof(missedBeats), "missedBeats must be at least 2");

        var channel = Channel.CreateUnbounded<PresenceChange>(new UnboundedChannelOptions { SingleReader = true });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var monitor = new Monitor(interval, missedBeats, channel.Writer);

        var watcher = Task.Run(async () =>
        {
            try
            {
                var (records, readRev) = await client.ListAllWithRevisionAsync(KeyPrefix, cts.Token).ConfigureAwait(false);
                foreach (var r in records) monitor.Beat(NameOf(r.Key));
                await foreach (var ev in client.WatchAsync(KeyPrefix, readRev, cts.Token).ConfigureAwait(false))
                {
                    if (ev.Type == RetcdEventType.Put) monitor.Beat(NameOf(ev.Key));
                    else monitor.Goodbye(NameOf(ev.Key));
                }
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        var sweeper = Task.Run(async () =>
        {
            var tick = TimeSpan.FromMilliseconds(Math.Max(20, interval.TotalMilliseconds / 4));
            using var timer = new PeriodicTimer(tick);
            try
            {
                while (await timer.WaitForNextTickAsync(cts.Token).ConfigureAwait(false)) monitor.Sweep();
            }
            catch (OperationCanceledException)
            {
            }
        }, CancellationToken.None);

        try
        {
            await foreach (var change in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false)) yield return change;
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(watcher, sweeper).ConfigureAwait(false);
        }
    }

    private static string NameOf(string key) => key.StartsWith(KeyPrefix, StringComparison.Ordinal) ? key[KeyPrefix.Length..] : key;

    /// <summary>
    /// The silence rules: Up, Late after 1.5 intervals, Down after <c>missedBeats</c> intervals or a goodbye.
    /// No I/O: <see cref="WatchAsync"/> feeds it beats and goodbyes and calls <see cref="Sweep"/> on a timer.
    /// Internal, with injectable clocks, so a unit test can drive it with a fake clock.
    /// </summary>
    internal sealed class Monitor
    {
        private sealed class Entry
        {
            public PresenceState State;
            public long LastBeatMs;
            public DateTimeOffset LastBeatWall;
        }

        private readonly object _lock = new();
        private readonly Dictionary<string, Entry> _entries = new();
        private readonly double _lateMs;
        private readonly double _downMs;
        private readonly ChannelWriter<PresenceChange> _out;
        private readonly Func<long> _nowMs;
        private readonly Func<DateTimeOffset> _wall;

        public Monitor(TimeSpan interval, int missedBeats, ChannelWriter<PresenceChange> output,
            Func<long>? nowMs = null, Func<DateTimeOffset>? wall = null)
        {
            _lateMs = interval.TotalMilliseconds * 1.5;
            _downMs = interval.TotalMilliseconds * missedBeats;
            _out = output;
            _nowMs = nowMs ?? (() => Stopwatch.GetTimestamp() * 1000 / Stopwatch.Frequency);
            _wall = wall ?? (() => DateTimeOffset.UtcNow);
        }

        public void Beat(string name)
        {
            lock (_lock)
            {
                var now = _nowMs();
                var wall = _wall();
                if (!_entries.TryGetValue(name, out var e))
                {
                    _entries[name] = new Entry { State = PresenceState.Up, LastBeatMs = now, LastBeatWall = wall };
                    _out.TryWrite(new PresenceChange(name, PresenceState.Up, wall));
                    return;
                }
                var was = e.State;
                e.LastBeatMs = now;
                e.LastBeatWall = wall;
                e.State = PresenceState.Up;
                if (was is PresenceState.Late or PresenceState.Down) _out.TryWrite(new PresenceChange(name, PresenceState.Back, wall));
            }
        }

        public void Goodbye(string name)
        {
            lock (_lock)
            {
                if (_entries.TryGetValue(name, out var e) && e.State != PresenceState.Down)
                {
                    e.State = PresenceState.Down;
                    _out.TryWrite(new PresenceChange(name, PresenceState.Down, e.LastBeatWall));
                }
            }
        }

        public void Sweep()
        {
            lock (_lock)
            {
                var now = _nowMs();
                foreach (var (name, e) in _entries)
                {
                    var silent = now - e.LastBeatMs;
                    if (e.State == PresenceState.Up && silent >= _lateMs && silent < _downMs)
                    {
                        e.State = PresenceState.Late;
                        _out.TryWrite(new PresenceChange(name, PresenceState.Late, e.LastBeatWall));
                    }
                    else if (e.State is PresenceState.Up or PresenceState.Late && silent >= _downMs)
                    {
                        e.State = PresenceState.Down;
                        _out.TryWrite(new PresenceChange(name, PresenceState.Down, e.LastBeatWall));
                    }
                }
            }
        }
    }
}

/// <summary>A running heartbeat. Dispose to stop it.</summary>
public sealed class PresenceHeartbeat : IAsyncDisposable
{
    private readonly RetcdClient _client;
    private readonly string _name;
    private readonly bool _removeOnDispose;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private long _seq;
    private long _failures;
    private Exception? _lastError;

    /// <summary>Raised when a beat could not be written. The heartbeat keeps going.</summary>
    public event Action<Exception>? BeatFailed;

    internal PresenceHeartbeat(RetcdClient client, string name, TimeSpan interval, bool removeOnDispose)
    {
        _client = client;
        _name = name;
        _removeOnDispose = removeOnDispose;
        _loop = Task.Run(() => RunAsync(interval));
    }

    /// <summary>Beats attempted so far.</summary>
    public long Beats => Interlocked.Read(ref _seq);

    /// <summary>Beats that failed (including unknown outcomes).</summary>
    public long Failures => Interlocked.Read(ref _failures);

    public Exception? LastError => Volatile.Read(ref _lastError);

    private async Task RunAsync(TimeSpan interval)
    {
        var ct = _cts.Token;
        using var timer = new PeriodicTimer(interval);
        try
        {
            do
            {
                var seq = Interlocked.Increment(ref _seq);
                try
                {
                    var body = RetcdPresence.BuildBeat(_name, Environment.ProcessId, seq, DateTimeOffset.UtcNow);
                    await _client.PutAsync(RetcdPresence.KeyFor(_name), body, null, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _failures);
                    Volatile.Write(ref _lastError, ex);
                    BeatFailed?.Invoke(ex);
                }
            }
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _loop.ConfigureAwait(false);
        _cts.Dispose();
        if (!_removeOnDispose) return;
        try
        {
            await _client.DeleteAsync(RetcdPresence.KeyFor(_name)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _lastError, ex); // goodbye is best effort; the monitor will see silence instead
            BeatFailed?.Invoke(ex);
        }
    }
}
