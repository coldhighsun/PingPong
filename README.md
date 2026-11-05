# PingPong

[中文](https://github.com/coldhighsun/PingPong/blob/main/README.zh-CN.md) | English

Application-level ping/pong heartbeat for .NET (net10.0). Detects dead connections, measures round-trip time, and
tells you when the peer stops answering. **Both ends must use this library** (it defines its own frame format).

- `PingPong` – transport-independent core (`PingPongMonitor`, `IPingPongTransport`).
- `PingPong.WebSockets` – WebSocket adapter and a ready-made wrapper (`PingPongWebSocket`).
- `PingPong.Tcp` – TCP/IP adapter (`TcpTransport`) and a ready-made wrapper (`PingPongTcpConnection`) with length-prefixed framing. Works over `TcpClient` or any `Stream` (e.g. `SslStream` for TLS).

Other transports can be added by implementing `IPingPongTransport`.

## Install

All three assemblies ship in a single NuGet package:

```
dotnet add package PingPong.Heartbeat
```

## Sample

[samples/PingPong.Sample](https://github.com/coldhighsun/PingPong/blob/main/samples/PingPong.Sample/Program.cs) is a runnable demo (client is the Initiator via `PingPongWebSocket`, server is the Responder via the core `PingPongMonitor` with its own receive loop; the server hangs after 5 s so the client reports `ConnectionLost`):

```
dotnet run --project samples/PingPong.Sample
```

To run server and client as separate processes (`server`/`client` for WebSocket, `tcp-server`/`tcp-client` for TCP), build once and start each one with `--no-build` in its own terminal. Without it, a rebuild triggered by the second `dotnet run` fails on Windows because the first process has the exe locked.

```
dotnet build samples/PingPong.Sample
dotnet run --no-build --project samples/PingPong.Sample -- server
dotnet run --no-build --project samples/PingPong.Sample -- client
```

## Quick start (WebSocket)

Works for a `ClientWebSocket` and for a server-side socket accepted by ASP.NET Core alike.

Typical setup: client = `Initiator` (default), server = `Responder` (`new PingPongOptions { Role = PingPongRole.Responder }`).

```csharp
await using var conn = new PingPongWebSocket(webSocket, new PingPongOptions
{
    Interval = TimeSpan.FromSeconds(15),
    Timeout = TimeSpan.FromSeconds(10),
    MaxMissedPongs = 2,
    CloseOnTimeout = true,   // default false: only notify
});

conn.MessageReceived += (data, type) => { /* application messages only */ };
conn.PongReceived    += rtt => Console.WriteLine($"RTT {rtt.TotalMilliseconds} ms");
conn.ConnectionLost  += reason => Console.WriteLine($"Lost: {reason}");
conn.Start();

await conn.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text);
await conn.Completion;
```

Always send through `PingPongWebSocket.SendAsync` (sends are serialized; a WebSocket allows one send at a time).

## Quick start (TCP)

TCP is a byte stream, so `PingPong.Tcp` frames every message as a 4-byte big-endian length followed by the payload
(max 16 MiB). **Both ends must use `PingPong.Tcp`**, including for application messages.

```csharp
// Client: TcpClient (or any connected Stream, e.g. an SslStream for TLS). Ownership moves to the connection.
var tcp = new TcpClient();
await tcp.ConnectAsync("example.com", 5056);

await using var conn = new PingPongTcpConnection(tcp, new PingPongOptions { CloseOnTimeout = true });
conn.MessageReceived += data => { /* application messages only; memory valid during the call */ };
conn.PongReceived    += rtt => Console.WriteLine($"RTT {rtt.TotalMilliseconds} ms");
conn.ConnectionLost  += reason => Console.WriteLine($"Lost: {reason}");
conn.Closed          += ex => Console.WriteLine(ex is null ? "closed" : $"failed: {ex.Message}");
conn.Start();

await conn.SendAsync("hello"u8.ToArray());
await conn.Completion;
```

The server side is the same with `new PingPongOptions { Role = PingPongRole.Responder }` on an accepted `TcpClient`.
`ConnectionLost` is what catches half-open connections (cable pulled, NAT timeout) that TCP itself reports only after minutes.
`CloseAsync` closes the socket immediately (TCP has no close reason). Always send through `PingPongTcpConnection.SendAsync`.
A send can be cancelled only while it waits for earlier sends; once its frame is being written it always completes (a half-written frame would corrupt the stream). `CloseAsync` aborts a write stuck on a peer that stopped reading.

To use the core with your own loop over TCP, send with `TcpTransport` and read with the public `FrameReader`:

```csharp
var transport = new TcpTransport(tcpClient);
await using var monitor = new PingPongMonitor(transport, options);
monitor.Start();

try
{
    using var reader = new FrameReader(tcpClient.GetStream(), monitor.NotifyReceived);
    while (await reader.ReadAsync(ct))
    {
        if (!monitor.TryHandleMessage(reader.Current.Span)) HandleApplicationMessage(reader.Current);
    }
}
finally
{
    // The transport owns the TcpClient: close it when the loop ends (peer closed, error, cancellation),
    // otherwise the socket stays open and the monitor keeps pinging a dead connection.
    await transport.CloseAsync("");
}
```

## Using the core with your own receive loop

```csharp
var monitor = new PingPongMonitor(myTransport, options);
monitor.ConnectionLost += reason => ...;
monitor.Start();

// for every received message:
if (!monitor.TryHandleMessage(bytes)) HandleApplicationMessage(bytes);
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `Interval` | 15 s | Time between pings |
| `Timeout` | 10 s | Wait for a pong before a ping counts as missed (must be ≤ `Interval`) |
| `PingOnlyWhenIdle` | true | Ping only after `IdleTimeout` without any inbound message (application data, ping or pong all count). Set to `false` to ping on every `Interval` |
| `IdleTimeout` | 10 s | Inbound silence required before pinging starts (used only when `PingOnlyWhenIdle` is true). The first ping goes out when it elapses; unanswered pings then repeat every `Interval`. **Behavior change:** since `PingOnlyWhenIdle` defaults to `true`, set it to `false` if you rely on a periodic ping/`PongReceived` RTT while traffic flows |
| `MaxMissedPongs` | 2 | Consecutive misses before `ConnectionLost` fires (once) |
| `CloseOnTimeout` | false | Also close the transport when the connection is lost |
| `Role` | `Initiator` | `Initiator` sends pings and detects loss (client). `Responder` only answers pings with pongs (server): no pings, no `PongReceived`/`ConnectionLost`. If both ends are initiators, each pings the other |

Misses are evaluated at each tick, so loss is reported roughly `MaxMissedPongs × Interval` after the first unanswered ping. With `PingOnlyWhenIdle`, that first ping follows the last inbound message by `IdleTimeout` exactly.
After `ConnectionLost` pinging stops; there is no automatic reconnect.

## Frame format

Binary message, 20 bytes, big endian: magic `PNG1` (ping) or `POG1` (pong), 8-byte sequence, 8-byte timestamp.
Text messages are never treated as heartbeats. Over TCP the heartbeat is one length-prefixed frame
(`00 00 00 14` + the 20 bytes). A binary application message of exactly 20 bytes starting with
one of those magics would be swallowed – avoid that prefix.

## License

[MIT](https://github.com/coldhighsun/PingPong/blob/main/LICENSE)
