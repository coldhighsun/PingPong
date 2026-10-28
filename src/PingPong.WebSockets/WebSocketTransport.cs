using System.Net.WebSockets;

namespace PingPong.WebSockets;

/// <summary>
/// Adapts a <see cref="WebSocket"/> to <see cref="IPingPongTransport"/>. Sends are serialized
/// because a WebSocket allows only one outstanding send at a time.
/// </summary>
public sealed class WebSocketTransport : IPingPongTransport
{
    private readonly WebSocket _socket;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public WebSocketTransport(WebSocket socket)
    {
        ArgumentNullException.ThrowIfNull(socket);
        _socket = socket;
    }

    public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        => SendAsync(data, WebSocketMessageType.Binary, cancellationToken);

    public async ValueTask SendAsync(ReadOnlyMemory<byte> data, WebSocketMessageType type, CancellationToken cancellationToken = default)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _socket.SendAsync(data, type, endOfMessage: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default)
    {
        if (_socket.State is not (WebSocketState.Open or WebSocketState.CloseReceived))
            return;
        // Take the send lock so the close does not overlap a send.
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, Truncate(reason), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    // Close reasons are limited to 123 bytes of UTF-8.
    private static string Truncate(string reason) => reason.Length <= 100 ? reason : reason[..100];
}
