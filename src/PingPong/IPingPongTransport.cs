namespace PingPong;

/// <summary>
/// Minimal outbound surface a transport (WebSocket, TCP, ...) must provide to <see cref="PingPongMonitor"/>.
/// Implementations must be safe for concurrent calls.
/// </summary>
public interface IPingPongTransport
{
    ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

    ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default);
}
