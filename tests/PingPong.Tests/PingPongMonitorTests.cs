using Microsoft.Extensions.Time.Testing;

namespace PingPong.Tests;

public class PingPongMonitorTests
{
    private sealed class FakeTransport : IPingPongTransport
    {
        public List<byte[]> Sent { get; } = new();
        public List<string> Closed { get; } = new();

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            lock (Sent) Sent.Add(data.ToArray());
            return ValueTask.CompletedTask;
        }

        public ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default)
        {
            lock (Closed) Closed.Add(reason);
            return ValueTask.CompletedTask;
        }

        public (PingPongKind Kind, ulong Seq, long Ts) Last()
        {
            lock (Sent)
            {
                Assert.True(PingPongProtocol.TryDecode(Sent[^1], out var k, out var s, out var t));
                return (k, s, t);
            }
        }
    }

    private static readonly PingPongOptions Opts = new()
    {
        Interval = TimeSpan.FromSeconds(10),
        Timeout = TimeSpan.FromSeconds(5),
        MaxMissedPongs = 2,
    };

    private static (PingPongMonitor, FakeTransport, FakeTimeProvider) Create(PingPongOptions? o = null)
    {
        var t = new FakeTransport();
        var time = new FakeTimeProvider();
        return (new PingPongMonitor(t, o ?? Opts, time), t, time);
    }

    private static byte[] PongFor(FakeTransport t)
    {
        var (_, seq, ts) = t.Last();
        return PingPongProtocol.Encode(PingPongKind.Pong, seq, ts);
    }

    [Fact]
    public async Task Sends_ping_every_interval()
    {
        var (m, t, time) = Create();
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Single(t.Sent);
        Assert.Equal(PingPongKind.Ping, t.Last().Kind);
        Assert.Equal(0UL, t.Last().Seq);

        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(2, t.Sent.Count);
        Assert.Equal(1UL, t.Last().Seq);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Pong_raises_event_with_rtt_and_resets_missed()
    {
        var (m, t, time) = Create();
        TimeSpan? rtt = null;
        var lost = false;
        m.PongReceived += r => rtt = r;
        m.ConnectionLost += _ => lost = true;
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10)); // ping 0
        time.Advance(TimeSpan.FromMilliseconds(40));
        Assert.True(m.TryHandleMessage(PongFor(t)));
        Assert.Equal(TimeSpan.FromMilliseconds(40), rtt);

        // Keep answering: never lost.
        for (var i = 0; i < 5; i++)
        {
            time.Advance(TimeSpan.FromMilliseconds(9960));
            time.Advance(TimeSpan.FromMilliseconds(10));
            m.TryHandleMessage(PongFor(t));
        }
        Assert.False(lost);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Raises_ConnectionLost_once_after_consecutive_misses()
    {
        var (m, t, time) = Create();
        var reasons = new List<string>();
        m.ConnectionLost += reasons.Add;
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10)); // ping 0
        time.Advance(TimeSpan.FromSeconds(10)); // ping 0 expired (missed 1), ping 1
        Assert.Empty(reasons);
        time.Advance(TimeSpan.FromSeconds(10)); // ping 1 expired (missed 2) -> lost
        Assert.Single(reasons);

        var sent = t.Sent.Count;
        time.Advance(TimeSpan.FromSeconds(60));
        Assert.Single(reasons);
        Assert.Equal(sent, t.Sent.Count); // no more pings after loss
        Assert.Empty(t.Closed);           // notify only by default
        await m.DisposeAsync();
    }

    [Fact]
    public async Task CloseOnTimeout_closes_transport()
    {
        var (m, t, time) = Create(new PingPongOptions
        {
            Interval = Opts.Interval, Timeout = Opts.Timeout, MaxMissedPongs = 1, CloseOnTimeout = true,
        });
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10));
        time.Advance(TimeSpan.FromSeconds(10));
        await Task.Yield();

        Assert.Single(t.Closed);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Incoming_ping_is_answered_with_matching_pong()
    {
        var (m, t, _) = Create();
        var ping = PingPongProtocol.Encode(PingPongKind.Ping, 7, 99);

        Assert.True(m.TryHandleMessage(ping));
        await Task.Yield();

        var (kind, seq, ts) = t.Last();
        Assert.Equal(PingPongKind.Pong, kind);
        Assert.Equal(7UL, seq);
        Assert.Equal(99L, ts);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Responder_never_pings_but_answers_pings()
    {
        var (m, t, time) = Create(new PingPongOptions
        {
            Interval = Opts.Interval, Timeout = Opts.Timeout, Role = PingPongRole.Responder,
        });
        var lost = false;
        m.ConnectionLost += _ => lost = true;
        m.Start();

        time.Advance(TimeSpan.FromMinutes(5));
        Assert.Empty(t.Sent);
        Assert.False(lost);

        Assert.True(m.TryHandleMessage(PingPongProtocol.Encode(PingPongKind.Ping, 3, 5)));
        await Task.Yield();
        Assert.Equal((PingPongKind.Pong, 3UL, 5L), t.Last());
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Initiator_pings_and_answers_pings()
    {
        var (m, t, time) = Create(new PingPongOptions
        {
            Interval = Opts.Interval, Timeout = Opts.Timeout, Role = PingPongRole.Initiator,
        });
        m.Start();
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(PingPongKind.Ping, t.Last().Kind);

        m.TryHandleMessage(PingPongProtocol.Encode(PingPongKind.Ping, 9, 1));
        await Task.Yield();
        Assert.Equal(PingPongKind.Pong, t.Last().Kind);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Application_data_is_not_handled()
    {
        var (m, _, _) = Create();
        Assert.False(m.TryHandleMessage("hello"u8));
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Unsolicited_pong_is_ignored()
    {
        var (m, _, _) = Create();
        var raised = false;
        m.PongReceived += _ => raised = true;

        Assert.True(m.TryHandleMessage(PingPongProtocol.Encode(PingPongKind.Pong, 123, 0)));
        Assert.False(raised);
        await m.DisposeAsync();
    }

    [Fact]
    public async Task Stop_and_dispose_stop_pings()
    {
        var (m, t, time) = Create();
        m.Start();
        time.Advance(TimeSpan.FromSeconds(10));
        m.Stop();
        time.Advance(TimeSpan.FromSeconds(100));
        Assert.Single(t.Sent);

        await m.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(m.Start);
    }

    [Fact]
    public void Invalid_options_throw()
    {
        var t = new FakeTransport();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PingPongMonitor(t, new PingPongOptions { Interval = TimeSpan.FromSeconds(1), Timeout = TimeSpan.FromSeconds(2) }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PingPongMonitor(t, new PingPongOptions { MaxMissedPongs = 0 }));
    }
}
