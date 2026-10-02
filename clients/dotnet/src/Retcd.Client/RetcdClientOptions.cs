namespace Retcd.Client;

/// <summary>Settings for <see cref="RetcdClient.Create"/>.</summary>
public sealed class RetcdClientOptions
{
    /// <summary>
    /// Client addresses of the nodes: "host:port" or "http://host:port". Give all of them.
    /// The client starts at the first and follows the leader.
    /// </summary>
    public IReadOnlyList<string> Endpoints { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Time budget for one call, including leader changes and retries. Default 10 s.
    /// A write that times out is reported as <see cref="UnknownOutcomeException"/>.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Pause before a watch reconnects after a lost stream. Default 500 ms.</summary>
    public TimeSpan WatchReconnectDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Health endpoints ("host:port"), one per entry of <see cref="Endpoints"/>, in the same order.
    /// Leave null to use client port plus 2 on the same host (the local-cluster layout).
    /// </summary>
    public IReadOnlyList<string>? HealthEndpoints { get; set; }
}
