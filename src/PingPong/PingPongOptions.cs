namespace PingPong;

/// <summary>Which side of the heartbeat this end plays. Every role answers incoming pings with a pong.</summary>
public enum PingPongRole
{
    /// <summary>Sends periodic pings and detects a lost connection (typically the client). Default.</summary>
    Initiator,

    /// <summary>Only answers pings; never sends pings or raises <see cref="PingPongMonitor.ConnectionLost"/> (typically the server).</summary>
    Responder,
}

public sealed class PingPongOptions
{
    /// <summary>Defaults to <see cref="PingPongRole.Initiator"/>. If both ends are initiators, each pings the other.</summary>
    public PingPongRole Role { get; set; } = PingPongRole.Initiator;

    /// <summary>Time between pings.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>How long to wait for a pong before a ping counts as missed. Must not exceed <see cref="Interval"/>.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Consecutive missed pongs after which the connection is considered lost.</summary>
    public int MaxMissedPongs { get; set; } = 2;

    /// <summary>Whether to close the transport when the connection is considered lost. Defaults to notify only.</summary>
    public bool CloseOnTimeout { get; set; }

    /// <summary>
    /// When true (default), pings are only sent after nothing has been received for <see cref="IdleTimeout"/>.
    /// Any inbound message (application data, ping or pong) counts as proof of life. Set to false to ping on every <see cref="Interval"/>.
    /// </summary>
    public bool PingOnlyWhenIdle { get; set; } = true;

    /// <summary>
    /// How long without any inbound message before pinging starts. Only used when <see cref="PingOnlyWhenIdle"/> is true.
    /// The first ping goes out right when this elapses; afterwards unanswered pings repeat every <see cref="Interval"/>.
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(10);

    internal void Validate()
    {
        if (PingOnlyWhenIdle && IdleTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(IdleTimeout), "IdleTimeout must be positive.");
        }

        if (!Enum.IsDefined(Role))
            throw new ArgumentOutOfRangeException(nameof(Role));
        if (Interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(Interval), "Interval must be positive.");
        if (Timeout <= TimeSpan.Zero || Timeout > Interval)
            throw new ArgumentOutOfRangeException(nameof(Timeout), "Timeout must be positive and not exceed Interval.");
        if (MaxMissedPongs < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxMissedPongs), "MaxMissedPongs must be at least 1.");
    }
}
