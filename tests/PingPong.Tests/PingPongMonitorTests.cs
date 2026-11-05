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
        PingOnlyWhenIdle = false,
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
            Interval = Opts.Interval, Timeout = Opts.Timeout, MaxMissedPongs = 1, CloseOnTimeout = true, PingOnlyWhenIdle = false,
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
            Interval = Opts.Interval, Timeout = Opts.Timeout, Role = PingPongRole.Initiator, PingOnlyWhenIdle = false,
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

    /// <summary>
    /// Creates on-demand options: 10 s interval, 25 s idle timeout.
    /// </summary>
    /// <returns>The options.</returns>
    private static PingPongOptions IdleOpts() => new()
    {
        Interval = TimeSpan.FromSeconds(10),
        Timeout = TimeSpan.FromSeconds(5),
        MaxMissedPongs = 2,
        PingOnlyWhenIdle = true,
        IdleTimeout = TimeSpan.FromSeconds(25),
    };

    /// <summary>
    /// Verifies the documented defaults of the on-demand settings.
    /// </summary>
    [Fact]
    public void Options_Defaults_PingOnlyWhenIdleOnWithTenSecondIdleTimeout()
    {
        var o = new PingPongOptions();

        Assert.True(o.PingOnlyWhenIdle);
        Assert.Equal(TimeSpan.FromSeconds(10), o.IdleTimeout);
    }

    /// <summary>
    /// Inbound application data keeps the link "alive", so no ping is sent.
    /// </summary>
    [Fact]
    public async Task OnTick_TrafficFlowing_SendsNoPing()
    {
        var (m, t, time) = Create(IdleOpts());
        m.Start();

        for (var i = 0; i < 10; i++)
        {
            time.Advance(TimeSpan.FromSeconds(10));
            m.TryHandleMessage("data"u8);
        }

        Assert.Empty(t.Sent);
        await m.DisposeAsync();
    }

    /// <summary>
    /// With no traffic, the first ping goes out exactly when the idle timeout expires.
    /// </summary>
    [Fact]
    public async Task OnTick_NoTraffic_PingsExactlyAtIdleTimeout()
    {
        var (m, t, time) = Create(IdleOpts());
        m.Start();

        time.Advance(TimeSpan.FromSeconds(24));
        Assert.Empty(t.Sent);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Single(t.Sent);
        Assert.Equal(PingPongKind.Ping, t.Last().Kind);
        await m.DisposeAsync();
    }

    /// <summary>
    /// When the idle timeout is shorter than the interval, the first ping is not delayed to the interval.
    /// </summary>
    [Fact]
    public async Task OnTick_IdleTimeoutShorterThanInterval_PingsAtIdleTimeout()
    {
        var (m, t, time) = Create(new PingPongOptions());
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Single(t.Sent);
        await m.DisposeAsync();
    }

    /// <summary>
    /// Traffic that bypasses <see cref="PingPongMonitor.TryHandleMessage"/> can still defer pinging.
    /// </summary>
    [Fact]
    public async Task NotifyReceived_RefreshesIdleTimer_DefersPing()
    {
        var (m, t, time) = Create(IdleOpts());
        m.Start();

        time.Advance(TimeSpan.FromSeconds(20));
        m.NotifyReceived();
        time.Advance(TimeSpan.FromSeconds(10)); // original tick at 25 s sees only 10 s of idle
        Assert.Empty(t.Sent);

        time.Advance(TimeSpan.FromSeconds(15)); // 25 s after the notification
        Assert.Single(t.Sent);
        await m.DisposeAsync();
    }

    /// <summary>
    /// After an idle ping is answered, the link counts as active again and pinging stops.
    /// </summary>
    [Fact]
    public async Task TryHandleMessage_PongAfterIdlePing_ReturnsToQuiet()
    {
        var (m, t, time) = Create(IdleOpts());
        m.Start();
        time.Advance(TimeSpan.FromSeconds(25));
        Assert.Single(t.Sent);

        time.Advance(TimeSpan.FromMilliseconds(40));
        m.TryHandleMessage(PongFor(t));

        time.Advance(TimeSpan.FromSeconds(10));
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Single(t.Sent);
        await m.DisposeAsync();
    }

    /// <summary>
    /// An idle link whose pings go unanswered is still reported lost after the configured misses.
    /// </summary>
    [Fact]
    public async Task OnTick_IdleAndNoPong_RaisesConnectionLost()
    {
        var (m, _, time) = Create(IdleOpts());
        var reasons = new List<string>();
        m.ConnectionLost += reasons.Add;
        m.Start();

        time.Advance(TimeSpan.FromSeconds(25)); // ping 0
        time.Advance(TimeSpan.FromSeconds(10)); // ping 0 missed (1), ping 1
        Assert.Empty(reasons);

        time.Advance(TimeSpan.FromSeconds(10)); // ping 1 missed (2) -> lost
        Assert.Single(reasons);
        await m.DisposeAsync();
    }

    /// <summary>
    /// Restarting the monitor restarts the idle window instead of pinging immediately.
    /// </summary>
    [Fact]
    public async Task Start_AfterStop_RestartsIdleWindow()
    {
        var (m, t, time) = Create(IdleOpts());
        m.Start();
        time.Advance(TimeSpan.FromSeconds(25));
        m.TryHandleMessage(PongFor(t));
        m.Stop();

        m.Start();
        time.Advance(TimeSpan.FromSeconds(24));
        Assert.Single(t.Sent);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(2, t.Sent.Count);
        await m.DisposeAsync();
    }

    /// <summary>
    /// With the switch off, pings continue on every interval regardless of inbound traffic.
    /// </summary>
    [Fact]
    public async Task OnTick_PingOnlyWhenIdleDisabled_PingsDespiteTraffic()
    {
        var (m, t, time) = Create();
        m.Start();

        time.Advance(TimeSpan.FromSeconds(10));
        m.TryHandleMessage("data"u8);
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(2, t.Sent.Count);
        await m.DisposeAsync();
    }

    /// <summary>
    /// A non-positive idle timeout is rejected only while the switch is on.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveIdleTimeout_ThrowsOnlyWhenEnabled()
    {
        var t = new FakeTransport();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PingPongMonitor(t, new PingPongOptions { IdleTimeout = TimeSpan.Zero }));
        _ = new PingPongMonitor(t, new PingPongOptions { PingOnlyWhenIdle = false, IdleTimeout = TimeSpan.Zero });
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
