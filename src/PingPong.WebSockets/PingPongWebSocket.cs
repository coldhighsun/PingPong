using System.Buffers;
using System.Net.WebSockets;

namespace PingPong.WebSockets;

/// <summary>
/// Wraps a connected <see cref="WebSocket"/> (client or server side): owns the receive loop,
/// handles ping/pong internally and delivers only application messages.
/// </summary>
public sealed class PingPongWebSocket : IAsyncDisposable
{
    private const int MaxMessageSize = 16 * 1024 * 1024;

    private readonly WebSocket _socket;
    private readonly WebSocketTransport _transport;
    private readonly PingPongMonitor _monitor;
    private readonly CancellationTokenSource _cts = new();
    private readonly PingPongOptions _options;
    private readonly TimeProvider _timeProvider;
    private Task? _receiveLoop;

    public PingPongWebSocket(WebSocket socket, PingPongOptions? options = null, TimeProvider? timeProvider = null)
    {
        _socket = socket;
        _options = options ?? new PingPongOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _transport = new WebSocketTransport(socket);
        _monitor = new PingPongMonitor(_transport, options, timeProvider);
        _monitor.PongReceived += rtt => PongReceived?.Invoke(rtt);
        _monitor.ConnectionLost += reason =>
        {
            ConnectionLost?.Invoke(reason);
            if (_options.CloseOnTimeout)
                _ = AbortIfPeerDoesNotCloseAsync();
        };
    }

    // The monitor has sent a close frame; a hung peer will never answer it, so give it
    // Timeout to do so and then cancel the receive loop, which aborts the socket.
    private async Task AbortIfPeerDoesNotCloseAsync()
    {
        try
        {
            await Task.Delay(_options.Timeout, _timeProvider, _cts.Token).ConfigureAwait(false);
            _cts.Cancel();
        }
        catch (Exception)
        {
            // Disposed or already closed.
        }
    }

    /// <summary>An application message arrived (ping/pong frames are never delivered).</summary>
    public event Action<ReadOnlyMemory<byte>, WebSocketMessageType>? MessageReceived;

    public event Action<TimeSpan>? PongReceived;

    public event Action<string>? ConnectionLost;

    /// <summary>Raised when the receive loop ends because the socket closed or failed.</summary>
    public event Action<Exception?>? Closed;

    /// <summary>Starts the receive loop and the heartbeat.</summary>
    public void Start()
    {
        if (_receiveLoop is not null) return;
        _receiveLoop = Task.Run(ReceiveLoopAsync);
        _monitor.Start();
    }

    /// <summary>Completes when the receive loop has ended.</summary>
    public Task Completion => _receiveLoop ?? Task.CompletedTask;

    public ValueTask SendAsync(ReadOnlyMemory<byte> data, WebSocketMessageType type = WebSocketMessageType.Binary, CancellationToken cancellationToken = default)
        => _transport.SendAsync(data, type, cancellationToken);

    public ValueTask CloseAsync(string reason = "", CancellationToken cancellationToken = default)
        => _transport.CloseAsync(reason, cancellationToken);

    private async Task ReceiveLoopAsync()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(8192);
        Exception? error = null;
        try
        {
            var ct = _cts.Token;
            while (!ct.IsCancellationRequested && _socket.State is WebSocketState.Open or WebSocketState.CloseSent)
            {
                var length = 0;
                ValueWebSocketReceiveResult result;
                do
                {
                    if (length == buffer.Length)
                    {
                        if (length >= MaxMessageSize)
                            throw new InvalidOperationException("Message too large.");
                        var bigger = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                        buffer.AsSpan(0, length).CopyTo(bigger);
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = bigger;
                    }
                    result = await _socket.ReceiveAsync(buffer.AsMemory(length), ct).ConfigureAwait(false);
                    length += result.Count;
                } while (!result.EndOfMessage);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    if (_socket.State == WebSocketState.CloseReceived)
                        await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None).ConfigureAwait(false);
                    break;
                }

                var message = buffer.AsMemory(0, length);
                if (result.MessageType == WebSocketMessageType.Binary && _monitor.TryHandleMessage(message.Span))
                    continue;

                MessageReceived?.Invoke(message, result.MessageType);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            _monitor.Stop();
            Closed?.Invoke(error);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _monitor.DisposeAsync().ConfigureAwait(false);
        if (_receiveLoop is not null)
        {
            try { await _receiveLoop.ConfigureAwait(false); } catch { }
        }
        _cts.Dispose();
    }
}
