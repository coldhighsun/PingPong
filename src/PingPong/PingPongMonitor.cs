namespace PingPong;

/// <summary>
/// Sends periodic pings, answers incoming pings, and reports round-trip times and lost connections.
/// Feed every received message to <see cref="TryHandleMessage"/>.
/// </summary>
public sealed class PingPongMonitor : IAsyncDisposable
{
    private readonly IPingPongTransport _transport;
    private readonly PingPongOptions _options;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private readonly Dictionary<ulong, long> _outstanding = new();
    private readonly CancellationTokenSource _cts = new();

    private ITimer? _timer;
    private ulong _nextSeq;
    private int _missed;
    private bool _lost;
    private bool _disposed;

    public PingPongMonitor(IPingPongTransport transport, PingPongOptions? options = null, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
        _options = options ?? new PingPongOptions();
        _options.Validate();
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Raised when a pong arrives, with the measured round-trip time.</summary>
    public event Action<TimeSpan>? PongReceived;

    /// <summary>Raised once when <see cref="PingPongOptions.MaxMissedPongs"/> consecutive pongs were missed.</summary>
    public event Action<string>? ConnectionLost;

    public void Start()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_timer is not null || _options.Role == PingPongRole.Responder) return;
            _timer = _time.CreateTimer(OnTick, null, _options.Interval, _options.Interval);
        }
    }

    public void Stop()
    {
        lock (_gate)
        {
            _timer?.Dispose();
            _timer = null;
        }
    }

    /// <summary>
    /// Returns true if <paramref name="data"/> was a ping/pong frame (handled internally); false if it is application data.
    /// </summary>
    public bool TryHandleMessage(ReadOnlySpan<byte> data)
    {
        if (!PingPongProtocol.TryDecode(data, out var kind, out var seq, out var timestamp))
            return false;

        if (kind == PingPongKind.Ping)
        {
            _ = SendSafeAsync(PingPongProtocol.Encode(PingPongKind.Pong, seq, timestamp));
            return true;
        }

        TimeSpan? rtt = null;
        lock (_gate)
        {
            if (_outstanding.Remove(seq, out var sentAt))
            {
                rtt = _time.GetElapsedTime(sentAt);
                _missed = 0;
                // Older pings that were never answered are superseded by this pong.
                foreach (var old in _outstanding.Keys.Where(k => k < seq).ToList())
                    _outstanding.Remove(old);
            }
        }

        if (rtt is { } value)
            PongReceived?.Invoke(value);
        return true;
    }

    private void OnTick(object? state)
    {
        string? lostReason = null;
        ulong seq;
        long now = _time.GetTimestamp();

        lock (_gate)
        {
            if (_disposed || _lost || _timer is null) return;

            var expired = _outstanding.Where(p => _time.GetElapsedTime(p.Value, now) >= _options.Timeout)
                                      .Select(p => p.Key).ToList();
            foreach (var key in expired)
            {
                _outstanding.Remove(key);
                _missed++;
            }

            if (_missed >= _options.MaxMissedPongs)
            {
                _lost = true;
                _timer.Dispose();
                _timer = null;
                lostReason = $"No pong received for {_missed} consecutive pings.";
                seq = 0;
            }
            else
            {
                seq = _nextSeq++;
                _outstanding[seq] = now;
            }
        }

        if (lostReason is not null)
        {
            ConnectionLost?.Invoke(lostReason);
            if (_options.CloseOnTimeout)
                _ = CloseSafeAsync(lostReason);
            return;
        }

        _ = SendSafeAsync(PingPongProtocol.Encode(PingPongKind.Ping, seq, now));
    }

    private async Task SendSafeAsync(byte[] frame)
    {
        try
        {
            await _transport.SendAsync(frame, _cts.Token).ConfigureAwait(false);
        }
        catch
        {
            // A failed send surfaces as a missed pong; nothing else to do here.
        }
    }

    private async Task CloseSafeAsync(string reason)
    {
        try
        {
            await _transport.CloseAsync(reason, _cts.Token).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposed) return ValueTask.CompletedTask;
            _disposed = true;
            _timer?.Dispose();
            _timer = null;
        }
        _cts.Cancel();
        _cts.Dispose();
        return ValueTask.CompletedTask;
    }
}
