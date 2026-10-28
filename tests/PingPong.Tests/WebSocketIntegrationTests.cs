using System.Net;
using System.Net.WebSockets;
using PingPong.WebSockets;

namespace PingPong.Tests;

public class WebSocketIntegrationTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static PingPongOptions Fast(bool close = false) => new()
    {
        Interval = TimeSpan.FromMilliseconds(100),
        Timeout = TimeSpan.FromMilliseconds(80),
        MaxMissedPongs = 2,
        CloseOnTimeout = close,
    };

    /// <summary>Starts a loopback server; hands the accepted server-side socket to <paramref name="onAccept"/>.</summary>
    private static async Task<(WebSocket Client, HttpListener Listener)> ConnectAsync(Func<WebSocket, Task> onAccept)
    {
        var port = new Random().Next(20000, 40000);
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        _ = Task.Run(async () =>
        {
            var ctx = await listener.GetContextAsync();
            var ws = (await ctx.AcceptWebSocketAsync(null)).WebSocket;
            await onAccept(ws);
        });

        var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri($"ws://localhost:{port}/"), CancellationToken.None);
        return (client, listener);
    }

    [Fact]
    public async Task Both_sides_exchange_heartbeats_and_pass_through_app_messages()
    {
        var serverRtt = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverGotMsg = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        PingPongWebSocket? server = null;

        var (clientSocket, listener) = await ConnectAsync(ws =>
        {
            server = new PingPongWebSocket(ws, Fast());
            server.PongReceived += r => serverRtt.TrySetResult(r);
            server.MessageReceived += (data, type) =>
                serverGotMsg.TrySetResult(System.Text.Encoding.UTF8.GetString(data.Span));
            server.Start();
            return Task.CompletedTask;
        });

        await using var client = new PingPongWebSocket(clientSocket, Fast());
        var clientRtt = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var clientGotMsgs = new List<string>();
        client.PongReceived += r => clientRtt.TrySetResult(r);
        client.MessageReceived += (data, _) => { lock (clientGotMsgs) clientGotMsgs.Add(System.Text.Encoding.UTF8.GetString(data.Span)); };
        client.Start();

        Assert.True((await clientRtt.Task.WaitAsync(Wait)) >= TimeSpan.Zero);
        Assert.True((await serverRtt.Task.WaitAsync(Wait)) >= TimeSpan.Zero);

        await client.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text);
        Assert.Equal("hello", await serverGotMsg.Task.WaitAsync(Wait));
        lock (clientGotMsgs) Assert.Empty(clientGotMsgs); // heartbeats never leak to the app

        await server!.DisposeAsync();
        listener.Stop();
    }

    [Fact]
    public async Task Silent_peer_triggers_ConnectionLost_and_optionally_closes()
    {
        // Server accepts but never reads, so no pong is ever returned.
        var serverSocket = new TaskCompletionSource<WebSocket>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (clientSocket, listener) = await ConnectAsync(ws =>
        {
            serverSocket.SetResult(ws);
            return Task.Delay(Timeout.Infinite);
        });

        await using var client = new PingPongWebSocket(clientSocket, Fast(close: true));
        var lost = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.ConnectionLost += r => lost.TrySetResult(r);
        client.Start();

        await lost.Task.WaitAsync(Wait);

        // CloseOnTimeout: client initiated a close, visible to the peer as CloseReceived once it reads.
        var deadline = DateTime.UtcNow + Wait;
        while (clientSocket.State == WebSocketState.Open && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        Assert.NotEqual(WebSocketState.Open, clientSocket.State);

        // The peer never acknowledges the close, yet the connection must still be torn down.
        await client.Completion.WaitAsync(Wait);

        listener.Stop();
    }
}
