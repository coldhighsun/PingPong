using System.Buffers;
using System.Buffers.Binary;

namespace PingPong.Tcp;

/// <summary>
/// Reads the length-prefixed frames written by <see cref="TcpTransport"/> (4-byte big-endian payload
/// length followed by the payload, at most 16 MiB) from a stream into a pooled buffer that is reused
/// between reads. Use it to build your own receive loop around <see cref="PingPongMonitor"/>.
/// Not thread-safe: use one reader per stream and read from one task at a time.
/// </summary>
public sealed class FrameReader : IDisposable
{
    /// <summary>
    /// Buffer size used initially and after shrinking.
    /// </summary>
    private const int DefaultBufferSize = 8192;

    /// <summary>
    /// Buffers larger than this are given back once traffic is small again.
    /// </summary>
    private const int ShrinkThreshold = 64 * 1024;

    /// <summary>
    /// Number of consecutive small frames after which an oversized buffer is shrunk. Requiring
    /// several avoids reallocating a large buffer when big messages are interleaved with heartbeats.
    /// </summary>
    private const int ShrinkAfterSmallFrames = 16;

    /// <summary>
    /// The stream frames are read from.
    /// </summary>
    private readonly Stream _stream;

    /// <summary>
    /// Scratch space for the length prefix.
    /// </summary>
    private readonly byte[] _header = new byte[LengthPrefixedFraming.HeaderSize];

    /// <summary>
    /// Pooled buffer that holds the current payload.
    /// </summary>
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);

    /// <summary>
    /// Length of the current payload inside <see cref="_buffer"/>.
    /// </summary>
    private int _length;

    /// <summary>
    /// Number of consecutive frames that would have fit the default buffer.
    /// </summary>
    private int _smallFrames;

    /// <summary>
    /// Set once <see cref="Dispose"/> has been called.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="FrameReader"/> class.
    /// </summary>
    /// <param name="stream">The stream to read frames from. It is not disposed by the reader.</param>
    public FrameReader(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        _stream = stream;
    }

    /// <summary>
    /// Gets the payload of the last frame read. It is only valid until the next call to
    /// <see cref="ReadAsync"/> or <see cref="Dispose"/>.
    /// </summary>
    public ReadOnlyMemory<byte> Current => _buffer.AsMemory(0, _length);

    /// <summary>
    /// Gets the size of the pooled buffer currently held, for tests.
    /// </summary>
    internal int BufferCapacity => _buffer.Length;

    /// <summary>
    /// Reads the next frame.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the read.</param>
    /// <returns><see langword="true"/> if a frame was read; <see langword="false"/> on a clean end of stream between frames.</returns>
    /// <exception cref="EndOfStreamException">The stream ended in the middle of a frame.</exception>
    /// <exception cref="InvalidDataException">The announced payload length is negative or exceeds the maximum.</exception>
    /// <exception cref="ObjectDisposedException">The reader has been disposed.</exception>
    public async ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _length = 0;

        var read = 0;
        while (read < _header.Length)
        {
            var count = await _stream.ReadAsync(_header.AsMemory(read), cancellationToken).ConfigureAwait(false);
            if (count == 0)
            {
                if (read == 0)
                {
                    return false;
                }

                throw new EndOfStreamException("Stream ended inside a frame header.");
            }

            read += count;
        }

        var length = BinaryPrimitives.ReadInt32BigEndian(_header);
        if (length < 0 || length > LengthPrefixedFraming.MaxMessageSize)
        {
            throw new InvalidDataException($"Invalid frame length {length}.");
        }

        // Saturate at the threshold so a long run of small frames can never overflow the counter.
        _smallFrames = length <= DefaultBufferSize ? Math.Min(_smallFrames + 1, ShrinkAfterSmallFrames) : 0;
        if (_buffer.Length > ShrinkThreshold && _smallFrames >= ShrinkAfterSmallFrames)
        {
            // A big message inflated the buffer long ago; give the memory back.
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
            _smallFrames = 0;
        }
        else if (_buffer.Length < length)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(length);
        }

        await _stream.ReadExactlyAsync(_buffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
        _length = length;
        return true;
    }

    /// <summary>
    /// Returns the pooled buffer. The underlying stream is left open.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
        var buffer = _buffer;
        _buffer = [];
        _length = 0;
        if (buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
