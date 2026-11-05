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
        PingOnlyWhenIdle = false,
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

        Assert.True((await clientRtt.Task.WaitAsync(Wait, TestCancellation.Token)) >= TimeSpan.Zero);
        Assert.True((await serverRtt.Task.WaitAsync(Wait, TestCancellation.Token)) >= TimeSpan.Zero);

        await client.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text, TestCancellation.Token);
        Assert.Equal("hello", await serverGotMsg.Task.WaitAsync(Wait, TestCancellation.Token));
        lock (clientGotMsgs) Assert.Empty(clientGotMsgs); // heartbeats never leak to the app

        await server!.DisposeAsync();
        listener.Stop();
    }

    /// <summary>
    /// Text frames from the peer count as activity, so an on-demand client sends no pings while they keep arriving.
    /// </summary>
    [Fact]
    public async Task Receive_TextTrafficFlowing_OnDemandClientSendsNoPing()
    {
        var pingsSeenByServer = 0;
        var serverDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var (clientSocket, listener) = await ConnectAsync(async ws =>
        {
            var reader = Task.Run(async () =>
            {
                var buffer = new byte[256];
                while (ws.State == WebSocketState.Open)
                {
                    var r = await ws.ReceiveAsync(buffer, CancellationToken.None);
                    if (r.MessageType == WebSocketMessageType.Binary)
                    {
                        Interlocked.Increment(ref pingsSeenByServer);
                    }
                    else if (r.MessageType == WebSocketMessageType.Close)
                    {
                        break;
                    }
                }
            });

            for (var i = 0; i < 30; i++)
            {
                await ws.SendAsync("tick"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            serverDone.TrySetResult();
            await reader;
        });

        await using var client = new PingPongWebSocket(clientSocket, new PingPongOptions
        {
            Interval = TimeSpan.FromMilliseconds(100),
            Timeout = TimeSpan.FromMilliseconds(80),
            PingOnlyWhenIdle = true,
            IdleTimeout = TimeSpan.FromMilliseconds(1500), // generous margin over the 100 ms send gap
        });
        client.Start();

        await serverDone.Task.WaitAsync(Wait, TestCancellation.Token);

        Assert.Equal(0, Volatile.Read(ref pingsSeenByServer));
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

        await lost.Task.WaitAsync(Wait, TestCancellation.Token);

        // CloseOnTimeout: client initiated a close, visible to the peer as CloseReceived once it reads.
        var deadline = DateTime.UtcNow + Wait;
        while (clientSocket.State == WebSocketState.Open && DateTime.UtcNow < deadline)
            await Task.Delay(20, TestCancellation.Token);
        Assert.NotEqual(WebSocketState.Open, clientSocket.State);

        // The peer never acknowledges the close, yet the connection must still be torn down.
        await client.Completion.WaitAsync(Wait, TestCancellation.Token);

        listener.Stop();
    }
}
