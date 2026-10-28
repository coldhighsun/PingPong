using System.Buffers.Binary;

namespace PingPong;

public enum PingPongKind
{
    Ping,
    Pong,
}

/// <summary>
/// Binary frame layout: 4-byte magic ("PNG1" / "POG1") + 8-byte sequence + 8-byte timestamp (big endian).
/// </summary>
internal static class PingPongProtocol
{
    public const int FrameLength = 20;

    private static ReadOnlySpan<byte> PingMagic => "PNG1"u8;
    private static ReadOnlySpan<byte> PongMagic => "POG1"u8;

    public static byte[] Encode(PingPongKind kind, ulong sequence, long timestamp)
    {
        var buf = new byte[FrameLength];
        (kind == PingPongKind.Ping ? PingMagic : PongMagic).CopyTo(buf);
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(4), sequence);
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(12), timestamp);
        return buf;
    }

    public static bool TryDecode(ReadOnlySpan<byte> data, out PingPongKind kind, out ulong sequence, out long timestamp)
    {
        kind = default;
        sequence = 0;
        timestamp = 0;

        if (data.Length != FrameLength)
            return false;

        var magic = data[..4];
        if (magic.SequenceEqual(PingMagic))
            kind = PingPongKind.Ping;
        else if (magic.SequenceEqual(PongMagic))
            kind = PingPongKind.Pong;
        else
            return false;

        sequence = BinaryPrimitives.ReadUInt64BigEndian(data[4..]);
        timestamp = BinaryPrimitives.ReadInt64BigEndian(data[12..]);
        return true;
    }
}
