using System.Text.Json;
using System.Threading.Channels;
using Retcd.Client;

namespace Retcd.Client.Tests;

public class PresenceUnitTests
{
    [Fact]
    public void Beat_json_has_exactly_name_pid_seq_sent_at_in_that_order()
    {
        var json = RetcdPresence.BuildBeat("web-1", 4242, 7, new DateTimeOffset(2026, 9, 30, 12, 34, 56, 789, TimeSpan.Zero));
        Assert.Equal("{\"name\":\"web-1\",\"pid\":4242,\"seq\":7,\"sent_at\":\"2026-09-30T12:34:56.789Z\"}", json);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(new[] { "name", "pid", "seq", "sent_at" }, doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray());
    }

    [Fact]
    public void Beat_time_is_utc_even_for_a_local_offset()
    {
        var json = RetcdPresence.BuildBeat("x", 1, 1, new DateTimeOffset(2026, 9, 30, 5, 0, 0, TimeSpan.FromHours(-7)));
        Assert.Contains("\"sent_at\":\"2026-09-30T12:00:00.000Z\"", json);
    }

    [Fact]
    public void Key_is_presence_slash_name()
    {
        Assert.Equal("presence/web-1", RetcdPresence.KeyFor("web-1"));
    }

    private sealed class Rig
    {
        public long Now;
        public readonly Channel<PresenceChange> Ch = Channel.CreateUnbounded<PresenceChange>();
        public readonly RetcdPresence.Monitor M;

        public Rig(int interval = 1000, int missed = 3)
        {
            M = new RetcdPresence.Monitor(TimeSpan.FromMilliseconds(interval), missed, Ch.Writer, () => Now, () => DateTimeOffset.UnixEpoch.AddMilliseconds(Now));
        }

        public List<(string, PresenceState)> Drain()
        {
            var l = new List<(string, PresenceState)>();
            while (Ch.Reader.TryRead(out var c)) l.Add((c.Name, c.State));
            return l;
        }
    }

    [Fact]
    public void Silence_goes_Up_then_Late_then_Down_then_Back()
    {
        var r = new Rig();
        r.M.Beat("a");
        Assert.Equal(new[] { ("a", PresenceState.Up) }, r.Drain());

        r.Now = 1400; r.M.Sweep();
        Assert.Empty(r.Drain());                       // 1.4 intervals: still fine
        r.Now = 1500; r.M.Sweep();
        Assert.Equal(new[] { ("a", PresenceState.Late) }, r.Drain());
        r.Now = 2900; r.M.Sweep();
        Assert.Empty(r.Drain());                       // Late is reported once
        r.Now = 3000; r.M.Sweep();
        Assert.Equal(new[] { ("a", PresenceState.Down) }, r.Drain());
        r.Now = 9000; r.M.Sweep();
        Assert.Empty(r.Drain());                       // Down is reported once

        r.M.Beat("a");
        Assert.Equal(new[] { ("a", PresenceState.Back) }, r.Drain());
    }

    [Fact]
    public void A_beat_while_Late_is_Back_and_resets_the_clock()
    {
        var r = new Rig();
        r.M.Beat("a"); r.Drain();
        r.Now = 1600; r.M.Sweep(); r.Drain();          // Late
        r.M.Beat("a");
        Assert.Equal(new[] { ("a", PresenceState.Back) }, r.Drain());
        r.Now = 1600 + 1400; r.M.Sweep();
        Assert.Empty(r.Drain());
    }

    [Fact]
    public void Steady_beats_say_nothing_after_Up()
    {
        var r = new Rig();
        for (var t = 0; t < 10_000; t += 1000)
        {
            r.Now = t;
            r.M.Beat("a");
            r.M.Sweep();
        }
        Assert.Equal(new[] { ("a", PresenceState.Up) }, r.Drain());
    }

    [Fact]
    public void Deleted_key_is_Down_at_once_and_a_later_beat_is_Back()
    {
        var r = new Rig();
        r.M.Beat("a"); r.Drain();
        r.Now = 10; r.M.Goodbye("a");
        Assert.Equal(new[] { ("a", PresenceState.Down) }, r.Drain());
        r.M.Goodbye("a");
        Assert.Empty(r.Drain());
        r.M.Beat("a");
        Assert.Equal(new[] { ("a", PresenceState.Back) }, r.Drain());
    }

    [Fact]
    public void Names_are_tracked_separately()
    {
        var r = new Rig();
        r.M.Beat("a");
        r.Now = 1000; r.M.Beat("b");
        r.Drain();
        r.Now = 3000; r.M.Sweep();                     // a silent 3000, b silent 2000
        Assert.Equal(new[] { ("a", PresenceState.Down), ("b", PresenceState.Late) }.OrderBy(x => x.Item1), r.Drain().OrderBy(x => x.Item1));
    }

    [Fact]
    public async Task Missed_beats_below_two_is_refused()
    {
        var client = RetcdClient.Create(new RetcdClientOptions { Endpoints = new[] { "127.0.0.1:1" } });
        var it = RetcdPresence.WatchAsync(client, TimeSpan.FromSeconds(1), missedBeats: 1).GetAsyncEnumerator();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await it.MoveNextAsync());
        client.Dispose();
    }
}
