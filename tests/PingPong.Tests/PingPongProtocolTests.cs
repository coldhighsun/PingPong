using PingPong;

namespace PingPong.Tests;

public class PingPongProtocolTests
{
    [Theory]
    [InlineData(PingPongKind.Ping)]
    [InlineData(PingPongKind.Pong)]
    public void RoundTrip(PingPongKind kind)
    {
        var buf = PingPongProtocol.Encode(kind, 42, 123456789L);

        Assert.Equal(PingPongProtocol.FrameLength, buf.Length);
        Assert.True(PingPongProtocol.TryDecode(buf, out var k, out var seq, out var ts));
        Assert.Equal(kind, k);
        Assert.Equal(42UL, seq);
        Assert.Equal(123456789L, ts);
    }

    [Fact]
    public void Rejects_wrong_length()
    {
        var buf = PingPongProtocol.Encode(PingPongKind.Ping, 1, 1);
        Assert.False(PingPongProtocol.TryDecode(buf.AsSpan(0, buf.Length - 1), out _, out _, out _));
        Assert.False(PingPongProtocol.TryDecode(new byte[buf.Length + 1], out _, out _, out _));
        Assert.False(PingPongProtocol.TryDecode(ReadOnlySpan<byte>.Empty, out _, out _, out _));
    }

    [Fact]
    public void Rejects_wrong_magic()
    {
        var buf = PingPongProtocol.Encode(PingPongKind.Ping, 1, 1);
        buf[0] ^= 0xFF;
        Assert.False(PingPongProtocol.TryDecode(buf, out _, out _, out _));
    }
}
