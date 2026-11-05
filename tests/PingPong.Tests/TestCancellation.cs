namespace PingPong.Tests;

/// <summary>
/// Shortcut for the cancellation token of the currently running xUnit test.
/// </summary>
internal static class TestCancellation
{
    /// <summary>
    /// Gets the token that is cancelled when the current test run is cancelled.
    /// </summary>
    public static CancellationToken Token => TestContext.Current.CancellationToken;
}
