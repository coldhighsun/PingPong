using System.Buffers.Binary;
using PingPong.Tcp;

namespace PingPong.Tests;

/// <summary>
/// Tests for <see cref="LengthPrefixedFraming"/> and <see cref="FrameReader"/>.
/// </summary>
public class LengthPrefixedFramingTests
{
    /// <summary>
    /// Writes <paramref name="size"/> bytes as a frame and reads them back.
    /// </summary>
    /// <param name="size">The payload size.</param>
    /// <returns>A task that completes when the test has finished.</returns>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5000)]
    [InlineData(100_000)]
    public async Task WriteAsync_ThenReadAsync_RoundTripsPayload(int size)
    {
        // Arrange
        var payload = new byte[size];
        new Random(42).NextBytes(payload);
        using var stream = new MemoryStream();
        await LengthPrefixedFraming.WriteAsync(stream, payload, CancellationToken.None);
        stream.Position = 0;
        using var reader = new FrameReader(stream);

        // Act
        var hasFrame = await reader.ReadAsync(CancellationToken.None);

        // Assert
        Assert.True(hasFrame);
        Assert.Equal(payload, reader.Current.ToArray());
    }

    /// <summary>
    /// Verifies that several frames written back to back are read in order.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_MultipleFrames_ReturnsEachInOrderThenEof()
    {
        // Arrange
        using var stream = new MemoryStream();
        await LengthPrefixedFraming.WriteAsync(stream, new byte[] { 1, 2, 3 }, CancellationToken.None);
        await LengthPrefixedFraming.WriteAsync(stream, new byte[] { 9 }, CancellationToken.None);
        stream.Position = 0;
        using var reader = new FrameReader(stream);

        // Act & Assert
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.Current.ToArray());
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        Assert.Equal(new byte[] { 9 }, reader.Current.ToArray());
        Assert.False(await reader.ReadAsync(CancellationToken.None));
    }

    /// <summary>
    /// Verifies that the buffer inflated by a big message is released after sustained small traffic,
    /// but not earlier (so interleaved heartbeats do not cause reallocation).
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_SmallFramesAfterBigOne_ShrinksBufferOnlyAfterSustainedSmallTraffic()
    {
        // Arrange
        const int smallFrames = 16;
        using var stream = new MemoryStream();
        await LengthPrefixedFraming.WriteAsync(stream, new byte[200_000], CancellationToken.None);
        for (var i = 0; i < smallFrames; i++)
        {
            await LengthPrefixedFraming.WriteAsync(stream, new byte[20], CancellationToken.None);
        }

        stream.Position = 0;
        using var reader = new FrameReader(stream);

        // Act & Assert
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        Assert.True(reader.BufferCapacity >= 200_000);

        for (var i = 1; i < smallFrames; i++)
        {
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            Assert.True(reader.BufferCapacity >= 200_000);
        }

        Assert.True(await reader.ReadAsync(CancellationToken.None));
        Assert.True(reader.BufferCapacity <= 8192);
        Assert.Equal(20, reader.Current.Length);
    }

    /// <summary>
    /// Verifies that a big message interleaved with small ones keeps the large buffer.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_BigMessagesInterleavedWithSmallOnes_KeepsLargeBuffer()
    {
        // Arrange
        using var stream = new MemoryStream();
        for (var i = 0; i < 5; i++)
        {
            await LengthPrefixedFraming.WriteAsync(stream, new byte[200_000], CancellationToken.None);
            await LengthPrefixedFraming.WriteAsync(stream, new byte[20], CancellationToken.None);
        }

        stream.Position = 0;
        using var reader = new FrameReader(stream);

        // Act & Assert
        while (await reader.ReadAsync(CancellationToken.None))
        {
            Assert.True(reader.BufferCapacity >= 200_000);
        }
    }

    /// <summary>
    /// Verifies that reading from a disposed reader fails instead of renting an unreturnable buffer.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_AfterDispose_ThrowsObjectDisposed()
    {
        // Arrange
        using var stream = new MemoryStream();
        await LengthPrefixedFraming.WriteAsync(stream, new byte[100], CancellationToken.None);
        stream.Position = 0;
        var reader = new FrameReader(stream);
        reader.Dispose();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await reader.ReadAsync(CancellationToken.None));
        Assert.True(reader.Current.IsEmpty);
    }

    /// <summary>
    /// Verifies that an empty stream is a clean end of stream.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_EmptyStream_ReturnsFalse()
    {
        // Arrange
        using var reader = new FrameReader(new MemoryStream());

        // Act
        var hasFrame = await reader.ReadAsync(CancellationToken.None);

        // Assert
        Assert.False(hasFrame);
        Assert.True(reader.Current.IsEmpty);
    }

    /// <summary>
    /// Verifies that a stream ending inside the header is an error.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_TruncatedHeader_ThrowsEndOfStream()
    {
        // Arrange
        using var reader = new FrameReader(new MemoryStream([0, 0]));

        // Act & Assert
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await reader.ReadAsync(CancellationToken.None));
    }

    /// <summary>
    /// Verifies that a stream ending inside the payload is an error.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task ReadAsync_TruncatedPayload_ThrowsEndOfStream()
    {
        // Arrange
        using var reader = new FrameReader(new MemoryStream([0, 0, 0, 10, 1, 2]));

        // Act & Assert
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await reader.ReadAsync(CancellationToken.None));
    }

    /// <summary>
    /// Verifies that invalid announced lengths are rejected before any allocation.
    /// </summary>
    /// <param name="length">The announced length.</param>
    /// <returns>A task that completes when the test has finished.</returns>
    [Theory]
    [InlineData(LengthPrefixedFraming.MaxMessageSize + 1)]
    [InlineData(-1)]
    public async Task ReadAsync_InvalidLength_ThrowsInvalidData(int length)
    {
        // Arrange
        var header = new byte[LengthPrefixedFraming.HeaderSize];
        BinaryPrimitives.WriteInt32BigEndian(header, length);
        using var reader = new FrameReader(new MemoryStream(header));

        // Act & Assert
        await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.ReadAsync(CancellationToken.None));
    }

    /// <summary>
    /// Verifies that writing an oversized message is rejected.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task WriteAsync_MessageTooLarge_ThrowsArgumentOutOfRange()
    {
        // Arrange
        using var stream = new MemoryStream();
        var data = new byte[LengthPrefixedFraming.MaxMessageSize + 1];

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await LengthPrefixedFraming.WriteAsync(stream, data, CancellationToken.None));
        Assert.Equal(0, stream.Length);
    }

    /// <summary>
    /// Verifies the on-wire layout: big-endian length followed by the payload.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task WriteAsync_Payload_WritesBigEndianLengthPrefix()
    {
        // Arrange
        using var stream = new MemoryStream();

        // Act
        await LengthPrefixedFraming.WriteAsync(stream, new byte[] { 0xAA, 0xBB }, CancellationToken.None);

        // Assert
        Assert.Equal(new byte[] { 0, 0, 0, 2, 0xAA, 0xBB }, stream.ToArray());
    }
}
