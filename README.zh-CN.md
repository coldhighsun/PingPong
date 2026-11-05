# PingPong

[English](README.md) | 中文

面向 .NET (net10.0) 的应用层 ping/pong 心跳库。用于检测失效连接、测量往返时延（RTT），并在对端不再应答时通知你。
**通信双方都必须使用本库**（它定义了自己的帧格式）。

- `PingPong` – 与传输层无关的核心库（`PingPongMonitor`、`IPingPongTransport`）。
- `PingPong.WebSockets` – WebSocket 适配器及开箱即用的封装（`PingPongWebSocket`）。
- `PingPong.Tcp` – TCP/IP 适配器（`TcpTransport`）及带长度前缀分帧的开箱即用封装（`PingPongTcpConnection`）。可用于 `TcpClient` 或任意 `Stream`（例如用 `SslStream` 实现 TLS）。

其他传输方式可通过实现 `IPingPongTransport` 来添加。

## 安装

三个程序集打包在同一个 NuGet 包中：

```
dotnet add package PingPong.Heartbeat
```

## 示例

[samples/PingPong.Sample](samples/PingPong.Sample/Program.cs) 是一个可运行的演示（客户端通过 `PingPongWebSocket` 作为 Initiator，服务端使用核心 `PingPongMonitor` 与自己的接收循环作为 Responder；服务端会在 5 秒后挂起，客户端随之报告 `ConnectionLost`）：

```
dotnet run --project samples/PingPong.Sample
```

如果要让服务端和客户端作为独立进程运行（WebSocket 用 `server`/`client`，TCP 用 `tcp-server`/`tcp-client`），请先构建一次，再在各自的终端里用 `--no-build` 启动。否则第二个 `dotnet run` 触发重新编译时，会因为 exe 被第一个进程占用而在 Windows 上失败。

```
dotnet build samples/PingPong.Sample
dotnet run --no-build --project samples/PingPong.Sample -- server
dotnet run --no-build --project samples/PingPong.Sample -- client
```

## 快速开始（WebSocket）

适用于 `ClientWebSocket`，也适用于 ASP.NET Core 接受的服务端 socket。

典型配置：客户端 = `Initiator`（默认），服务端 = `Responder`（`new PingPongOptions { Role = PingPongRole.Responder }`）。

```csharp
await using var conn = new PingPongWebSocket(webSocket, new PingPongOptions
{
    Interval = TimeSpan.FromSeconds(15),
    Timeout = TimeSpan.FromSeconds(10),
    MaxMissedPongs = 2,
    CloseOnTimeout = true,   // 默认 false：仅通知
});

conn.MessageReceived += (data, type) => { /* 仅包含应用层消息 */ };
conn.PongReceived    += rtt => Console.WriteLine($"RTT {rtt.TotalMilliseconds} ms");
conn.ConnectionLost  += reason => Console.WriteLine($"Lost: {reason}");
conn.Start();

await conn.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text);
await conn.Completion;
```

请始终通过 `PingPongWebSocket.SendAsync` 发送（发送会被串行化；WebSocket 同一时刻只允许一个发送操作）。

## 快速开始（TCP）

TCP 是字节流，因此 `PingPong.Tcp` 将每条消息封装为：4 字节大端长度前缀 + 负载（最大 16 MiB）。
**通信双方都必须使用 `PingPong.Tcp`**，应用层消息同样如此。

```csharp
// 客户端：TcpClient（或任意已连接的 Stream，例如用于 TLS 的 SslStream）。所有权转移给连接对象。
var tcp = new TcpClient();
await tcp.ConnectAsync("example.com", 5056);

await using var conn = new PingPongTcpConnection(tcp, new PingPongOptions { CloseOnTimeout = true });
conn.MessageReceived += data => { /* 仅包含应用层消息；内存仅在调用期间有效 */ };
conn.PongReceived    += rtt => Console.WriteLine($"RTT {rtt.TotalMilliseconds} ms");
conn.ConnectionLost  += reason => Console.WriteLine($"Lost: {reason}");
conn.Closed          += ex => Console.WriteLine(ex is null ? "closed" : $"failed: {ex.Message}");
conn.Start();

await conn.SendAsync("hello"u8.ToArray());
await conn.Completion;
```

服务端写法相同，只需在接受到的 `TcpClient` 上使用 `new PingPongOptions { Role = PingPongRole.Responder }`。
`ConnectionLost` 可以捕获半开连接（网线被拔、NAT 超时），而 TCP 自身往往要几分钟后才会发现这类问题。
`CloseAsync` 会立即关闭 socket（TCP 没有关闭原因）。请始终通过 `PingPongTcpConnection.SendAsync` 发送。
发送操作只有在等待前序发送时才能被取消；一旦其帧开始写入就一定会完成（写了一半的帧会破坏数据流）。`CloseAsync` 会中止因对端停止读取而卡住的写入。

若要在 TCP 上配合自己的接收循环使用核心库，请用 `TcpTransport` 发送，并用公开的 `FrameReader` 读取：

```csharp
var transport = new TcpTransport(tcpClient);
await using var monitor = new PingPongMonitor(transport, options);
monitor.Start();

try
{
    using var reader = new FrameReader(tcpClient.GetStream());
    while (await reader.ReadAsync(ct))
    {
        if (!monitor.TryHandleMessage(reader.Current.Span)) HandleApplicationMessage(reader.Current);
    }
}
finally
{
    // transport 持有 TcpClient：循环结束时（对端关闭、出错、取消）需要关闭它，
    // 否则 socket 会一直保持打开，monitor 也会持续对一个已死的连接发送 ping。
    await transport.CloseAsync("");
}
```

## 配合自己的接收循环使用核心库

```csharp
var monitor = new PingPongMonitor(myTransport, options);
monitor.ConnectionLost += reason => ...;
monitor.Start();

// 对每一条收到的消息：
if (!monitor.TryHandleMessage(bytes)) HandleApplicationMessage(bytes);
```

## 配置项

| 选项 | 默认值 | 含义 |
|---|---|---|
| `Interval` | 15 秒 | 两次 ping 之间的间隔 |
| `Timeout` | 10 秒 | 等待 pong 的时长，超时则该次 ping 视为丢失（必须 ≤ `Interval`） |
| `MaxMissedPongs` | 2 | 连续丢失多少次后触发 `ConnectionLost`（仅触发一次） |
| `CloseOnTimeout` | false | 连接丢失时是否同时关闭传输层 |
| `Role` | `Initiator` | `Initiator` 发送 ping 并检测连接丢失（客户端）。`Responder` 只用 pong 应答 ping（服务端）：不发送 ping，也没有 `PongReceived`/`ConnectionLost`。如果两端都是 initiator，则双方互相 ping |

丢失判定在每个 tick 进行，因此大约在最后一次被应答的 ping 之后 `MaxMissedPongs × Interval` 时报告连接丢失。
`ConnectionLost` 之后 ping 会停止；不提供自动重连。

## 帧格式

二进制消息，共 20 字节，大端序：魔数 `PNG1`（ping）或 `POG1`（pong），8 字节序列号，8 字节时间戳。
文本消息永远不会被当作心跳。在 TCP 上，心跳是一个带长度前缀的帧（`00 00 00 14` + 上述 20 字节）。
恰好 20 字节且以上述魔数开头的二进制应用消息会被吞掉——请避免使用该前缀。

## 许可证

[MIT](LICENSE)
