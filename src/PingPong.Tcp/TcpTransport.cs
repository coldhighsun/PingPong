using System.Net.Sockets;

namespace PingPong.Tcp;

/// <summary>
/// Adapts a TCP connection (any <see cref="Stream"/>, e.g. <see cref="NetworkStream"/> or an
/// <see cref="System.Net.Security.SslStream"/>) to <see cref="IPingPongTransport"/>. Messages are
/// length-prefixed (4-byte big-endian) and sends are serialized so frames never interleave.
/// </summary>
public sealed class TcpTransport : IPingPongTransport
{
    /// <summary>
    /// The stream messages are written to.
    /// </summary>
    private readonly Stream _stream;

    /// <summary>
    /// Optional owner disposed together with the stream (the <see cref="TcpClient"/>).
    /// </summary>
    private readonly IDisposable? _owner;

    /// <summary>
    /// Serializes writes so frames are never interleaved.
    /// </summary>
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    /// <summary>
    /// Set to 1 once the transport has been closed.
    /// </summary>
    private int _closed;

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpTransport"/> class over a stream.
    /// </summary>
    /// <param name="stream">A connected, read/write stream. The transport takes ownership and disposes it on close.</param>
    public TcpTransport(Stream stream)
        : this(stream, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpTransport"/> class over a connected client.
    /// </summary>
    /// <param name="client">A connected client. The transport takes ownership and disposes it on close; if the constructor throws, the caller still owns it.</param>
    public TcpTransport(TcpClient client)
        : this((client ?? throw new ArgumentNullException(nameof(client))).GetStream(), client)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpTransport"/> class.
    /// </summary>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="owner">An object to dispose after the stream, or <see langword="null"/>.</param>
    private TcpTransport(Stream stream, IDisposable? owner)
    {
        ArgumentNullException.ThrowIfNull(stream);
        _stream = stream;
        _owner = owner;
    }

    /// <summary>
    /// Gets the underlying stream, used by the receive loop.
    /// </summary>
    internal Stream Stream => _stream;

    /// <summary>
    /// Gets a value indicating whether the transport has been closed locally.
    /// </summary>
    internal bool IsClosed => Volatile.Read(ref _closed) == 1;

    /// <summary>
    /// Sends one message as a length-prefixed frame. The token can cancel the send while it waits
    /// for earlier sends; once the frame has started to be written it can no longer be cancelled,
    /// because a half-written frame would corrupt the framing for every later message. A write
    /// stuck on a peer that stopped reading is aborted by <see cref="CloseAsync"/>.
    /// </summary>
    /// <param name="data">The message payload.</param>
    /// <param name="cancellationToken">A token to cancel the send before it starts writing.</param>
    /// <returns>A task that completes when the frame has been written.</returns>
    public async ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await LengthPrefixedFraming.WriteAsync(_stream, data, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Closes the connection. TCP has no close reason, so <paramref name="reason"/> is ignored.
    /// The send lock is deliberately not taken: a peer that stopped reading would otherwise block
    /// the close forever. Calling this more than once is harmless.
    /// </summary>
    /// <param name="reason">Ignored.</param>
    /// <param name="cancellationToken">Ignored; closing is immediate.</param>
    /// <returns>A task that completes when the stream has been disposed.</returns>
    public async ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default)
    {
        if (!TryMarkClosed())
        {
            return;
        }

        try
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
        catch (IOException)
        {
            // The connection is already broken; there is nothing left to flush.
        }

        _owner?.Dispose();
    }

    /// <summary>
    /// Synchronous variant of <see cref="CloseAsync"/>, used when a constructor has to clean up.
    /// </summary>
    internal void Close()
    {
        if (!TryMarkClosed())
        {
            return;
        }

        try
        {
            _stream.Dispose();
        }
        catch (IOException)
        {
            // The connection is already broken; there is nothing left to flush.
        }

        _owner?.Dispose();
    }

    /// <summary>
    /// Marks the transport as closed.
    /// </summary>
    /// <returns><see langword="true"/> for the first caller, which must perform the close; otherwise <see langword="false"/>.</returns>
    private bool TryMarkClosed() => Interlocked.Exchange(ref _closed, 1) == 0;
}
