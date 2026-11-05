using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using PingPong.Tcp;

namespace PingPong.Tests;

/// <summary>
/// Loopback integration tests for <see cref="PingPongTcpConnection"/>.
/// </summary>
public class TcpIntegrationTests
{
    /// <summary>
    /// How long a test waits for an expected event before failing.
    /// </summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Creates fast heartbeat options so tests finish quickly.
    /// </summary>
    /// <param name="role">The role of the side the options are for.</param>
    /// <param name="closeOnTimeout">Whether to close the transport when the connection is lost.</param>
    /// <returns>The options.</returns>
    private static PingPongOptions FastOptions(PingPongRole role, bool closeOnTimeout = false) => new()
    {
        Role = role,
        Interval = TimeSpan.FromMilliseconds(100),
        Timeout = TimeSpan.FromMilliseconds(80),
        MaxMissedPongs = 2,
        CloseOnTimeout = closeOnTimeout,
    };

    /// <summary>
    /// Opens a loopback TCP connection on an OS-assigned port.
    /// </summary>
    /// <returns>The client side and the accepted server side.</returns>
    private static async Task<(TcpClient Client, TcpClient Server)> ConnectAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var client = new TcpClient();
        var acceptTask = listener.AcceptTcpClientAsync();
        await client.ConnectAsync(IPAddress.Loopback, port);
        var server = await acceptTask.WaitAsync(WaitTimeout, TestCancellation.Token);
        return (client, server);
    }

    /// <summary>
    /// Verifies heartbeats flow both ways while application messages pass through untouched.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task Start_BothSidesInitiators_ExchangeHeartbeatsAndPassThroughAppMessages()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        await using var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Initiator));
        await using var server = new PingPongTcpConnection(serverSocket, FastOptions(PingPongRole.Initiator));

        var clientRtt = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverRtt = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverMessages = new List<string>();
        var serverGotMessage = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.PongReceived += rtt => clientRtt.TrySetResult(rtt);
        server.PongReceived += rtt => serverRtt.TrySetResult(rtt);
        server.MessageReceived += data =>
        {
            lock (serverMessages)
            {
                serverMessages.Add(System.Text.Encoding.UTF8.GetString(data.Span));
            }

            serverGotMessage.TrySetResult();
        };
        client.Start();
        server.Start();

        // Act
        await client.SendAsync("hello"u8.ToArray(), TestCancellation.Token);
        await serverGotMessage.Task.WaitAsync(WaitTimeout, TestCancellation.Token);
        var rttOnClient = await clientRtt.Task.WaitAsync(WaitTimeout, TestCancellation.Token);
        var rttOnServer = await serverRtt.Task.WaitAsync(WaitTimeout, TestCancellation.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(350), TestCancellation.Token); // several more heartbeats

        // Assert
        Assert.True(rttOnClient >= TimeSpan.Zero);
        Assert.True(rttOnServer >= TimeSpan.Zero);
        lock (serverMessages)
        {
            Assert.Equal(["hello"], serverMessages);
        }
    }

    /// <summary>
    /// Verifies that a peer that never answers triggers ConnectionLost and, with CloseOnTimeout, closes.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task Start_SilentPeer_RaisesConnectionLostAndClosesWhenConfigured()
    {
        // Arrange: the server side is a raw socket that never reads or answers.
        var (clientSocket, silentServer) = await ConnectAsync();
        using var _ = silentServer;
        await using var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Initiator, closeOnTimeout: true));
        var lost = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.ConnectionLost += reason => lost.TrySetResult(reason);
        client.Closed += ex => closed.TrySetResult(ex);

        // Act
        client.Start();
        var reason = await lost.Task.WaitAsync(WaitTimeout, TestCancellation.Token);
        var closeError = await closed.Task.WaitAsync(WaitTimeout, TestCancellation.Token);
        await client.Completion.WaitAsync(WaitTimeout, TestCancellation.Token);

        // Assert
        Assert.False(string.IsNullOrEmpty(reason));
        Assert.Null(closeError);
    }

    /// <summary>
    /// Verifies that the peer closing the connection ends the loop with a clean Closed event.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task Start_PeerCloses_RaisesClosedWithoutError()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        await using var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Responder));
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Closed += ex => closed.TrySetResult(ex);
        client.Start();

        // Act
        serverSocket.Dispose();
        var error = await closed.Task.WaitAsync(WaitTimeout, TestCancellation.Token);

        // Assert
        Assert.Null(error);
        await client.Completion.WaitAsync(WaitTimeout, TestCancellation.Token);
    }

    /// <summary>
    /// Verifies that a corrupt (oversized) frame header ends the connection with an error.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task Start_OversizedFrame_RaisesClosedWithInvalidData()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        using var _ = serverSocket;
        await using var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Responder));
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Closed += ex => closed.TrySetResult(ex);
        client.Start();

        // Act
        var header = new byte[LengthPrefixedFraming.HeaderSize];
        BinaryPrimitives.WriteInt32BigEndian(header, int.MaxValue);
        await serverSocket.GetStream().WriteAsync(header, TestCancellation.Token);
        var error = await closed.Task.WaitAsync(WaitTimeout, TestCancellation.Token);

        // Assert
        Assert.IsType<InvalidDataException>(error);
    }

    /// <summary>
    /// Verifies that disposing twice is a no-op.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        using var _ = serverSocket;
        var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Responder));
        client.Start();
        await client.DisposeAsync();

        // Act
        var second = await Record.ExceptionAsync(async () => await client.DisposeAsync());

        // Assert
        Assert.Null(second);
    }

    /// <summary>
    /// Verifies that invalid options release the stream the caller handed over.
    /// </summary>
    [Fact]
    public void Constructor_InvalidOptions_ThrowsAndClosesStream()
    {
        // Arrange
        var stream = new MemoryStream();
        var options = new PingPongOptions { Interval = TimeSpan.FromSeconds(1), Timeout = TimeSpan.FromSeconds(5) };

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new PingPongTcpConnection(stream, options));
        Assert.False(stream.CanWrite);
    }

    /// <summary>
    /// Verifies that a throwing Closed handler faults Completion but does not break disposal.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task DisposeAsync_ClosedHandlerThrew_DoesNotThrowAndCompletionIsFaulted()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Responder));
        client.Closed += _ => throw new InvalidOperationException("handler failed");
        client.Start();
        serverSocket.Dispose();

        // Act
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.Completion.WaitAsync(WaitTimeout, TestCancellation.Token));
        var disposeError = await Record.ExceptionAsync(async () => await client.DisposeAsync());

        // Assert
        Assert.Null(disposeError);
    }

    /// <summary>
    /// Verifies that disposing a started connection stops the loop without raising an error.
    /// </summary>
    /// <returns>A task that completes when the test has finished.</returns>
    [Fact]
    public async Task DisposeAsync_StartedConnection_EndsLoopWithoutError()
    {
        // Arrange
        var (clientSocket, serverSocket) = await ConnectAsync();
        using var _ = serverSocket;
        var client = new PingPongTcpConnection(clientSocket, FastOptions(PingPongRole.Responder));
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Closed += ex => closed.TrySetResult(ex);
        client.Start();

        // Act
        await client.DisposeAsync().AsTask().WaitAsync(WaitTimeout, TestCancellation.Token);

        // Assert
        Assert.Null(await closed.Task.WaitAsync(WaitTimeout, TestCancellation.Token));
    }
}
