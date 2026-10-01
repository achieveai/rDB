using Grpc.Net.Client;
using Pb = Retcd.V1;

namespace Retcd.Client;

/// <summary>Which node we talk to, and one gRPC channel per node. Thread-safe.</summary>
internal sealed class Transport : IDisposable
{
    private readonly object _lock = new();
    private readonly List<string> _endpoints = new();
    private readonly Dictionary<string, GrpcChannel> _channels = new();
    private readonly string _defaultScheme;
    private string _current;
    private bool _disposed;

    public Transport(IReadOnlyList<string> endpoints)
    {
        if (endpoints is null || endpoints.Count == 0)
        {
            throw new ArgumentException("give at least one endpoint, for example \"127.0.0.1:17302\"", nameof(endpoints));
        }
        _defaultScheme = new Uri(Normalize(endpoints[0], "http")).Scheme;
        foreach (var e in endpoints)
        {
            var n = Normalize(e, _defaultScheme);
            if (!_endpoints.Contains(n)) _endpoints.Add(n);
        }
        _current = _endpoints[0];
    }

    /// <summary>"host:port" becomes "http://host:port". Full URLs keep their scheme.</summary>
    public static string Normalize(string endpoint, string defaultScheme)
    {
        var e = endpoint.Trim();
        if (e.Length == 0) throw new ArgumentException("empty endpoint");
        if (!e.Contains("://", StringComparison.Ordinal)) e = $"{defaultScheme}://{e}";
        var authority = e[(e.IndexOf("://", StringComparison.Ordinal) + 3)..];
        if (!System.Text.RegularExpressions.Regex.IsMatch(authority, @":\d+(/.*)?$")
            || !Uri.TryCreate(e, UriKind.Absolute, out var uri) || uri.Port <= 0)
        {
            throw new ArgumentException($"endpoint \"{endpoint}\" is not host:port or http(s)://host:port");
        }
        return $"{uri.Scheme}://{uri.Host}:{uri.Port}";
    }

    public string Normalize(string endpoint) => Normalize(endpoint, _defaultScheme);

    public string Current
    {
        get { lock (_lock) return _current; }
    }

    public IReadOnlyList<string> Endpoints
    {
        get { lock (_lock) return _endpoints.ToArray(); }
    }

    /// <summary>Switch to this node (a leader hint). Unknown nodes are remembered.</summary>
    public void Use(string endpoint)
    {
        var n = Normalize(endpoint);
        lock (_lock)
        {
            if (!_endpoints.Contains(n)) _endpoints.Add(n);
            _current = n;
        }
    }

    /// <summary>Move to the next configured node.</summary>
    public void Rotate()
    {
        lock (_lock)
        {
            var i = _endpoints.IndexOf(_current);
            _current = _endpoints[(i + 1) % _endpoints.Count];
        }
    }

    /// <summary>Forget a node's channel so the next call connects fresh (a node that was down may be back).</summary>
    public void Drop(string endpoint)
    {
        GrpcChannel? ch;
        lock (_lock)
        {
            if (!_channels.Remove(endpoint, out ch)) return;
        }
        ch.Dispose();
    }

    public Pb.ConfigService.ConfigServiceClient ClientFor(string endpoint) =>
        new(ChannelFor(endpoint));

    /// <summary>
    /// Open the connection before a call is sent, so "could not connect" (nothing sent) is never confused with
    /// "sent, then timed out". Returns null when connected, else why it failed.
    /// </summary>
    public async Task<Exception?> ConnectAsync(string endpoint, DateTime deadlineUtc, CancellationToken ct)
    {
        var ch = ChannelFor(endpoint);
        var left = deadlineUtc - DateTime.UtcNow;
        if (left <= TimeSpan.Zero) return new TimeoutException("no time left to connect");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(left);
        try
        {
            await ch.ConnectAsync(cts.Token).ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            ct.ThrowIfCancellationRequested();
            return ex;
        }
    }

    private GrpcChannel ChannelFor(string endpoint)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_channels.TryGetValue(endpoint, out var ch))
            {
                ch = GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions
                {
                    MaxReceiveMessageSize = 16 * 1024 * 1024,
                    MaxSendMessageSize = 16 * 1024 * 1024,
                    HttpHandler = new SocketsHttpHandler
                    {
                        ConnectTimeout = TimeSpan.FromSeconds(3),
                        PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                        KeepAlivePingDelay = TimeSpan.FromSeconds(20),
                        KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                        KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                    },
                    DisposeHttpClient = true,
                });
                _channels[endpoint] = ch;
            }
            return ch;
        }
    }

    public void Dispose()
    {
        List<GrpcChannel> all;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            all = _channels.Values.ToList();
            _channels.Clear();
        }
        foreach (var c in all) c.Dispose();
    }
}
