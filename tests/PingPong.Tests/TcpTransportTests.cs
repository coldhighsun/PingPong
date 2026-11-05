using PingPong.Tcp;

namespace PingPong.Tests;

/// <summary>
/// Tests for <see cref="TcpTransport"/>.
/// </summary>
public class TcpTransportTests
{
    /// <summary>
    /// Verifies that concurrent sends never interleave their frames.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task SendAsync_ConcurrentSends_FramesAreNotInterleaved()
    {
        // Arrange
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        const int count = 50;

        // Act
        await Task.WhenAll(Enumerable.Range(0, count).Select(async i =>
        {
            await Task.Yield();
            await transport.SendAsync(Enumerable.Repeat((byte)i, 100 + i).ToArray());
        }));

        // Assert
        stream.Position = 0;
        using var reader = new FrameReader(stream);
        var seen = new HashSet<int>();
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var frame = reader.Current.ToArray();
            Assert.All(frame, b => Assert.Equal(frame[0], b));
            Assert.Equal(100 + frame[0], frame.Length);
            seen.Add(frame[0]);
        }

        Assert.Equal(count, seen.Count);
    }

    /// <summary>
    /// Verifies that closing disposes the stream and that closing again is harmless.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task CloseAsync_CalledTwice_DisposesStreamOnceWithoutThrowing()
    {
        // Arrange
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        // Act
        await transport.CloseAsync("bye");
        await transport.CloseAsync("again");

        // Assert
        Assert.True(transport.IsClosed);
        Assert.False(stream.CanWrite);
    }

    /// <summary>
    /// Verifies that sending after close fails instead of silently dropping data.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task SendAsync_AfterClose_Throws()
    {
        // Arrange
        var transport = new TcpTransport(new MemoryStream());
        await transport.CloseAsync("bye");

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await transport.SendAsync(new byte[] { 1 }));
    }

    /// <summary>
    /// Verifies that cancelling a send after its write has started does not leave a partial frame.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task SendAsync_CancelledWhileWriting_StillWritesCompleteFrame()
    {
        // Arrange
        var stream = new GatedStream();
        var transport = new TcpTransport(stream);
        using var cts = new CancellationTokenSource();
        var send = transport.SendAsync(new byte[] { 1, 2, 3 }, cts.Token).AsTask();
        await stream.WriteStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Act
        await cts.CancelAsync();
        stream.Gate.SetResult();
        await send.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(new byte[] { 0, 0, 0, 3, 1, 2, 3 }, stream.ToArray());
    }

    /// <summary>
    /// Verifies that a token that is already cancelled prevents anything from being written.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task SendAsync_AlreadyCancelled_ThrowsAndWritesNothing()
    {
        // Arrange
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await transport.SendAsync(new byte[] { 1 }, cts.Token));
        Assert.Equal(0, stream.Length);
    }

    /// <summary>
    /// A stream whose writes block until released, and which honors the cancellation token like a real socket stream.
    /// </summary>
    private sealed class GatedStream : MemoryStream
    {
        /// <summary>
        /// Completed when a write has started.
        /// </summary>
        public TaskCompletionSource WriteStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Completed by the test to let the blocked write finish.
        /// </summary>
        public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Blocks until <see cref="Gate"/> is completed or the token is cancelled, then writes.
        /// </summary>
        /// <param name="buffer">The data to write.</param>
        /// <param name="cancellationToken">A token that aborts the pending write.</param>
        /// <returns>A task that completes when the data has been written.</returns>
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            WriteStarted.TrySetResult();
            await Gate.Task.WaitAsync(cancellationToken);
            await base.WriteAsync(buffer, CancellationToken.None);
        }
    }

    /// <summary>
    /// Verifies argument validation.
    /// </summary>
    [Fact]
    public void Constructor_NullArguments_Throw()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new TcpTransport((Stream)null!));
        Assert.Throws<ArgumentNullException>(() => new TcpTransport((System.Net.Sockets.TcpClient)null!));
    }
}
