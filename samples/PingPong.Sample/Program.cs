// PingPong sample.
//
//   dotnet run --project samples/PingPong.Sample            # in-process demo: server + client
//
// To run server and client as separate processes, build once and start both with --no-build.
// Otherwise a rebuild triggered by the second "dotnet run" fails on Windows (MSB3027), because
// the first process has PingPong.Sample.exe locked.
//
//   dotnet build samples/PingPong.Sample
//   dotnet run --no-build --project samples/PingPong.Sample -- server  # server only (port 5055)
//   dotnet run --no-build --project samples/PingPong.Sample -- client  # client only, connects to port 5055
//   dotnet run --no-build --project samples/PingPong.Sample -- tcp-server  # TCP echo server (port 5056)
//   dotnet run --no-build --project samples/PingPong.Sample -- tcp-client  # TCP client, connects to port 5056
//
// Roles: the client is the Initiator (sends pings, measures RTT, detects a lost connection);
// the server is the Responder (only answers pings with pongs).
// The demo shows: RTT reporting, application messages passing through, and ConnectionLost
// when the server stops answering (we simulate that by stopping the server's receive loop).

using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using PingPong;
using PingPong.Tcp;
using PingPong.WebSockets;

const int Port = 5055;
const int TcpPort = 5056;
var url = $"ws://localhost:{Port}/";

// Short intervals so the demo finishes quickly; use the defaults (15 s / 10 s) in real apps.
var options = new PingPongOptions
{
    Interval = TimeSpan.FromSeconds(1),
    Timeout = TimeSpan.FromMilliseconds(800),
    MaxMissedPongs = 2,
    CloseOnTimeout = true, // default is false: only raise ConnectionLost
    Role = PingPongRole.Initiator, // client side
};

// The server only replies with pongs: no pings, no timeout detection.
var serverOptions = new PingPongOptions { Role = PingPongRole.Responder };

var mode = args.FirstOrDefault() ?? "demo";
switch (mode)
{
    case "server":
        await RunServerAsync(simulateFailureAfter: null, CancellationToken.None);
        break;
    case "client":
        await RunClientAsync();
        break;
    case "tcp-server":
        await RunTcpServerAsync();
        break;
    case "tcp-client":
        await RunTcpClientAsync();
        break;
    default:
        var serverTask = RunServerAsync(simulateFailureAfter: TimeSpan.FromSeconds(5), CancellationToken.None);
        await Task.Delay(300); // let the listener start
        await RunClientAsync();
        break;
}

// TCP: same wrapper shape as the WebSocket one; messages are length-prefixed on the wire.
async Task RunTcpClientAsync()
{
    var tcp = new TcpClient();
    await tcp.ConnectAsync("localhost", TcpPort);

    await using var conn = new PingPongTcpConnection(tcp, options);
    conn.PongReceived += rtt => Log("client", $"pong, RTT {rtt.TotalMilliseconds:F1} ms");
    conn.ConnectionLost += reason => Log("client", $"CONNECTION LOST: {reason}");
    conn.MessageReceived += data => Log("client", $"app message: {Encoding.UTF8.GetString(data.Span)}");
    conn.Closed += ex => Log("client", ex is null ? "closed" : $"closed with error: {ex.Message}");
    conn.Start();

    await conn.SendAsync("hello from tcp client"u8.ToArray());
    await conn.Completion;
}

async Task RunTcpServerAsync()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, TcpPort);
    listener.Start();
    Log("server", $"listening on tcp://localhost:{TcpPort}");

    var tcp = await listener.AcceptTcpClientAsync();
    listener.Stop();

    await using var conn = new PingPongTcpConnection(tcp, serverOptions);
    conn.MessageReceived += data =>
    {
        var text = Encoding.UTF8.GetString(data.Span);
        Log("server", $"app message: {text}");
        _ = conn.SendAsync(Encoding.UTF8.GetBytes($"echo: {text}"));
    };
    conn.Closed += ex => Log("server", ex is null ? "closed" : $"closed with error: {ex.Message}");
    conn.Start();

    await conn.Completion;
}

void Log(string who, string message) => Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {who,-6} {message}");

async Task RunClientAsync()
{
    var socket = new ClientWebSocket();
    await socket.ConnectAsync(new Uri(url), CancellationToken.None);

    await using var conn = new PingPongWebSocket(socket, options);
    conn.PongReceived += rtt => Log("client", $"pong, RTT {rtt.TotalMilliseconds:F1} ms");
    conn.ConnectionLost += reason => Log("client", $"CONNECTION LOST: {reason}");
    conn.MessageReceived += (data, _) => Log("client", $"app message: {Encoding.UTF8.GetString(data.Span)}");
    conn.Closed += ex => Log("client", ex is null ? "closed" : $"closed with error: {ex.Message}");
    conn.Start();

    await conn.SendAsync("hello from client"u8.ToArray(), WebSocketMessageType.Text);
    await conn.Completion; // ends when the connection closes (here: after CloseOnTimeout)
}

// The server uses the core API directly (PingPongMonitor + your own receive loop),
// to show the alternative to the PingPongWebSocket wrapper used by the client.
async Task RunServerAsync(TimeSpan? simulateFailureAfter, CancellationToken ct)
{
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://localhost:{Port}/");
    listener.Start();
    Log("server", $"listening on {url}");

    var ctx = await listener.GetContextAsync();
    var socket = (await ctx.AcceptWebSocketAsync(null)).WebSocket;

    var transport = new WebSocketTransport(socket);
    await using var monitor = new PingPongMonitor(transport, serverOptions);
    monitor.Start(); // a Responder starts no timer; it just answers incoming pings

    var started = DateTime.UtcNow;
    var buffer = new byte[8192];
    try
    {
        while (socket.State == WebSocketState.Open)
        {
            if (simulateFailureAfter is { } delay && DateTime.UtcNow - started > delay)
            {
                // Simulate a hung peer: stop reading (so no more pongs) but keep the socket open.
                Log("server", "simulating a hung server: no more pongs from here on");
                await Task.Delay(Timeout.Infinite, ct);
            }

            // (Messages are assumed to fit the buffer; the PingPongWebSocket wrapper handles large ones.)
            var result = await socket.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var message = buffer.AsMemory(0, result.Count);
            if (monitor.TryHandleMessage(message.Span)) continue; // heartbeat, handled

            var text = Encoding.UTF8.GetString(message.Span);
            Log("server", $"app message: {text}");
            await transport.SendAsync(Encoding.UTF8.GetBytes($"echo: {text}"), WebSocketMessageType.Text, ct);
        }

        Log("server", "closed");
    }
    catch (WebSocketException ex)
    {
        // With your own receive loop, a peer that vanishes without a close handshake surfaces here.
        Log("server", $"closed with error: {ex.Message}");
    }
}
