using System.Buffers;
using System.Buffers.Binary;

namespace PingPong.Tcp;

/// <summary>
/// Message framing for byte streams: a 4-byte big-endian payload length followed by the payload.
/// </summary>
internal static class LengthPrefixedFraming
{
    /// <summary>
    /// Size of the length prefix in bytes.
    /// </summary>
    public const int HeaderSize = 4;

    /// <summary>
    /// Largest accepted payload (16 MiB), matching the WebSocket adapter.
    /// </summary>
    public const int MaxMessageSize = 16 * 1024 * 1024;

    /// <summary>
    /// Writes one framed message to <paramref name="stream"/> with a single write call.
    /// </summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="data">The payload.</param>
    /// <param name="cancellationToken">A token to cancel the write.</param>
    /// <returns>A task that completes when the frame has been written and flushed.</returns>
    public static async ValueTask WriteAsync(Stream stream, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        if (data.Length > MaxMessageSize)
        {
            throw new ArgumentOutOfRangeException(nameof(data), data.Length, $"Message exceeds {MaxMessageSize} bytes.");
        }

        var frameLength = HeaderSize + data.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(frameLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(buffer, data.Length);
            data.CopyTo(buffer.AsMemory(HeaderSize));
            await stream.WriteAsync(buffer.AsMemory(0, frameLength), cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
