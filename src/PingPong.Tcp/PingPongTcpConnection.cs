using System.Net.Sockets;

namespace PingPong.Tcp;

/// <summary>
/// Wraps a connected TCP connection (client or server side): owns the receive loop, handles
/// ping/pong internally and delivers only application messages. The connection (and its stream)
/// is owned by this object and closed on dispose or when the receive loop ends.
/// </summary>
public sealed class PingPongTcpConnection : IAsyncDisposable
{
    /// <summary>
    /// Sends and closes the connection.
    /// </summary>
    private readonly TcpTransport _transport;

    /// <summary>
    /// Runs the heartbeat.
    /// </summary>
    private readonly PingPongMonitor _monitor;

    /// <summary>
    /// Cancels the receive loop.
    /// </summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>
    /// The receive loop task, or <see langword="null"/> before <see cref="Start"/>.
    /// </summary>
    private Task? _receiveLoop;

    /// <summary>
    /// Set to 1 once <see cref="DisposeAsync"/> has been called.
    /// </summary>
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PingPongTcpConnection"/> class over a client.
    /// </summary>
    /// <param name="client">A connected client; ownership is transferred.</param>
    /// <param name="options">Heartbeat options, or <see langword="null"/> for defaults.</param>
    /// <param name="timeProvider">Time source, or <see langword="null"/> for the system clock.</param>
    public PingPongTcpConnection(TcpClient client, PingPongOptions? options = null, TimeProvider? timeProvider = null)
        : this(new TcpTransport(client), options, timeProvider)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PingPongTcpConnection"/> class over a stream.
    /// </summary>
    /// <param name="stream">A connected stream (e.g. an <see cref="System.Net.Security.SslStream"/>); ownership is transferred.</param>
    /// <param name="options">Heartbeat options, or <see langword="null"/> for defaults.</param>
    /// <param name="timeProvider">Time source, or <see langword="null"/> for the system clock.</param>
    public PingPongTcpConnection(Stream stream, PingPongOptions? options = null, TimeProvider? timeProvider = null)
        : this(new TcpTransport(stream), options, timeProvider)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PingPongTcpConnection"/> class.
    /// </summary>
    /// <param name="transport">The transport to run over.</param>
    /// <param name="options">Heartbeat options, or <see langword="null"/> for defaults.</param>
    /// <param name="timeProvider">Time source, or <see langword="null"/> for the system clock.</param>
    private PingPongTcpConnection(TcpTransport transport, PingPongOptions? options, TimeProvider? timeProvider)
    {
        _transport = transport;
        try
        {
            _monitor = new PingPongMonitor(transport, options ?? new PingPongOptions(), timeProvider);
        }
        catch
        {
            // Ownership was already transferred to us, but the caller never gets this object to
            // dispose, so release the connection before reporting the invalid arguments.
            transport.Close();
            throw;
        }

        _monitor.PongReceived += rtt => PongReceived?.Invoke(rtt);

        // With CloseOnTimeout the monitor already closes the transport; closing a TCP stream
        // locally is immediate and unblocks the receive loop, so no extra abort logic is needed.
        _monitor.ConnectionLost += reason => ConnectionLost?.Invoke(reason);
    }

    /// <summary>
    /// Raised when an application message arrives. Heartbeat frames are never delivered. The
    /// memory is only valid for the duration of the call.
    /// </summary>
    public event Action<ReadOnlyMemory<byte>>? MessageReceived;

    /// <summary>
    /// Raised when a pong answers one of our pings, with the measured round-trip time.
    /// </summary>
    public event Action<TimeSpan>? PongReceived;

    /// <summary>
    /// Raised once when too many consecutive pongs were missed.
    /// </summary>
    public event Action<string>? ConnectionLost;

    /// <summary>
    /// Raised when the receive loop ends because the connection closed or failed. The argument is
    /// <see langword="null"/> for a clean close (by either side).
    /// </summary>
    public event Action<Exception?>? Closed;

    /// <summary>
    /// Gets a task that completes when the receive loop has ended.
    /// </summary>
    public Task Completion => _receiveLoop ?? Task.CompletedTask;

    /// <summary>
    /// Starts the receive loop and the heartbeat.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        if (_receiveLoop is not null)
        {
            return;
        }

        _receiveLoop = Task.Run(ReceiveLoopAsync);
        _monitor.Start();
    }

    /// <summary>
    /// Sends an application message. Always send through this method so frames are serialized.
    /// </summary>
    /// <param name="data">The message payload.</param>
    /// <param name="cancellationToken">A token to cancel the send.</param>
    /// <returns>A task that completes when the message has been written.</returns>
    public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        => _transport.SendAsync(data, cancellationToken);

    /// <summary>
    /// Closes the connection.
    /// </summary>
    /// <param name="reason">Ignored by TCP; kept for symmetry with other transports.</param>
    /// <param name="cancellationToken">A token to cancel the close.</param>
    /// <returns>A task that completes when the connection has been closed.</returns>
    public ValueTask CloseAsync(string reason = "", CancellationToken cancellationToken = default)
        => _transport.CloseAsync(reason, cancellationToken);

    /// <summary>
    /// Reads frames until the connection ends, feeding heartbeats to the monitor and raising
    /// <see cref="MessageReceived"/> for everything else.
    /// </summary>
    /// <returns>A task that completes when the loop has ended.</returns>
    private async Task ReceiveLoopAsync()
    {
        Exception? error = null;
        using var reader = new FrameReader(_transport.Stream, _monitor.NotifyReceived);
        try
        {
            var ct = _cts.Token;
            while (!ct.IsCancellationRequested && await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var message = reader.Current;
                if (_monitor.TryHandleMessage(message.Span))
                {
                    continue;
                }

                MessageReceived?.Invoke(message);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException || _transport.IsClosed || _cts.IsCancellationRequested)
        {
            // We closed or cancelled the connection ourselves; this is not an error.
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            _monitor.Stop();
            await _transport.CloseAsync(string.Empty).ConfigureAwait(false);
            Closed?.Invoke(error);
        }
    }

    /// <summary>
    /// Stops the heartbeat, closes the connection and waits for the receive loop to end. Calling it
    /// more than once is harmless. An exception thrown by a <see cref="Closed"/> handler does not
    /// escape from here (cleanup must complete); it stays observable through <see cref="Completion"/>.
    /// </summary>
    /// <returns>A task that completes when disposal has finished.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _cts.CancelAsync().ConfigureAwait(false);
        await _monitor.DisposeAsync().ConfigureAwait(false);
        await _transport.CloseAsync(string.Empty).ConfigureAwait(false);
        if (_receiveLoop is not null)
        {
            try
            {
                await _receiveLoop.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The loop only faults when a Closed handler threw; callers can observe that via
                // Completion, and it must not prevent the rest of the disposal.
            }
        }

        _cts.Dispose();
    }
}
