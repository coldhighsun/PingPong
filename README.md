# PingPong

Application-level ping/pong heartbeat for .NET (net10.0). Detects dead connections, measures round-trip time, and
tells you when the peer stops answering. **Both ends must use this library** (it defines its own frame format).

- `PingPong` – transport-independent core (`PingPongMonitor`, `IPingPongTransport`).
- `PingPong.WebSockets` – WebSocket adapter and a ready-made wrapper (`PingPongWebSocket`). TCP etc. can be added by implementing `IPingPongTransport`.

## Sample

[samples/PingPong.Sample](samples/PingPong.Sample/Program.cs) is a runnable demo (client is the Initiator via `PingPongWebSocket`, server is the Responder via the core `PingPongMonitor` with its own receive loop; the server hangs after 5 s so the client reports `ConnectionLost`):

```
dotnet run --project samples/PingPong.Sample
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
| `MaxMissedPongs` | 2 | Consecutive misses before `ConnectionLost` fires (once) |
| `CloseOnTimeout` | false | Also close the transport when the connection is lost |
| `Role` | `Initiator` | `Initiator` sends pings and detects loss (client). `Responder` only answers pings with pongs (server): no pings, no `PongReceived`/`ConnectionLost`. If both ends are initiators, each pings the other |

Misses are evaluated at each tick, so loss is reported roughly `MaxMissedPongs × Interval` after the last answered ping.
After `ConnectionLost` pinging stops; there is no automatic reconnect.

## Frame format

Binary message, 20 bytes, big endian: magic `PNG1` (ping) or `POG1` (pong), 8-byte sequence, 8-byte timestamp.
Text messages are never treated as heartbeats. A binary application message of exactly 20 bytes starting with
one of those magics would be swallowed – avoid that prefix.
