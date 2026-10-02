using Google.Protobuf;
using Grpc.Core;
using Pb = Retcd.V1;

namespace Retcd.Client.Tests;

/// <summary>
/// An in-process node for client tests, no sockets: Get, Put, Delete, List (paged) and Watch over keys
/// compared as raw bytes, so keys that are not valid UTF-8 stay distinct. <see cref="Attach"/> makes a client
/// whose every endpoint connects at once and answers from this one store.
/// </summary>
internal sealed class MemoryNode : CallInvoker
{
    private readonly SortedDictionary<byte[], Pb.Record> _kv = new(ByteOrder.Instance);
    private readonly List<Pb.Event> _history = new();
    private ulong _rev;

    /// <summary>When set, answers every List in place of the store (a scripted page walk).</summary>
    public Func<Pb.ListRequest, Pb.ListResponse>? ListOverride { get; set; }

    public IReadOnlyCollection<byte[]> Keys => _kv.Keys;

    /// <summary>A client over <paramref name="nodes"/> endpoints (127.0.0.1:1, :2, ...), all served by one new store.</summary>
    public static (RetcdClient Client, MemoryNode Node) Attach(int nodes = 1)
    {
        var node = new MemoryNode();
        var c = RetcdClient.Create(new RetcdClientOptions
        {
            Endpoints = Enumerable.Range(1, nodes).Select(i => $"127.0.0.1:{i}").ToArray(),
            WatchReconnectDelay = TimeSpan.FromMilliseconds(10),
        });
        c.TestTransport.FakeConnect = (_, _) => null;
        c.TestTransport.FakeInvoker = _ => node;
        return (c, node);
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        Task<TResponse> answer;
        try
        {
            object resp = request switch
            {
                Pb.GetRequest g => new Pb.GetResponse { Record = Find(g.Key), ReadRevision = _rev },
                Pb.PutRequest p => Put(p),
                Pb.DeleteRequest d => Delete(d),
                Pb.ListRequest l => ListOverride is { } o ? o(l) : List(l),
                _ => throw new NotSupportedException(method.FullName),
            };
            answer = Task.FromResult((TResponse)resp);
        }
        catch (Exception ex)
        {
            answer = Task.FromException<TResponse>(ex);
        }
        return new AsyncUnaryCall<TResponse>(answer, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        var w = (Pb.WatchRequest)(object)request!;
        var events = _history
            .Where(e => e.Revision > w.StartAfterRevision && e.Key.Span.StartsWith(w.Prefix.Span))
            .Select(e => new Pb.WatchResponse { Event = e })
            .ToList();
        var reader = (IAsyncStreamReader<TResponse>)(object)new Reader<Pb.WatchResponse>(events);
        return new AsyncServerStreamingCall<TResponse>(reader, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    private Pb.Record? Find(ByteString key) => _kv.TryGetValue(key.ToByteArray(), out var r) ? r : null;

    private Pb.MutationResponse Put(Pb.PutRequest p)
    {
        var old = Find(p.Key);
        if (p.HasExpectedModRevision && p.ExpectedModRevision != (old?.ModRevision ?? 0))
        {
            return new Pb.MutationResponse { Outcome = Pb.MutationOutcome.Conflict, Exists = old is not null, CurrentModRevision = old?.ModRevision ?? 0 };
        }
        _rev++;
        var rec = new Pb.Record { Key = p.Key, Value = p.Value, CreateRevision = old?.CreateRevision ?? _rev, ModRevision = _rev };
        _kv[p.Key.ToByteArray()] = rec;
        _history.Add(new Pb.Event { Revision = _rev, Key = p.Key, Put = rec });
        return new Pb.MutationResponse { Outcome = Pb.MutationOutcome.Applied, Revision = _rev };
    }

    private Pb.MutationResponse Delete(Pb.DeleteRequest d)
    {
        if (!_kv.Remove(d.Key.ToByteArray())) return new Pb.MutationResponse { Outcome = Pb.MutationOutcome.NotFound };
        _rev++;
        _history.Add(new Pb.Event { Revision = _rev, Key = d.Key, Delete = new Pb.Deleted() });
        return new Pb.MutationResponse { Outcome = Pb.MutationOutcome.Applied, Revision = _rev };
    }

    // The page token is the last key of the previous page.
    private Pb.ListResponse List(Pb.ListRequest l)
    {
        var after = l.HasPageToken ? l.PageToken.ToByteArray() : Array.Empty<byte>();
        var match = _kv.Where(e => e.Key.AsSpan().StartsWith(l.Prefix.Span) && (after.Length == 0 || ByteOrder.Instance.Compare(e.Key, after) > 0))
            .Select(e => e.Value).ToList();
        var resp = new Pb.ListResponse { ReadRevision = _rev };
        resp.Records.AddRange(match.Take((int)l.MaxItems));
        if (match.Count > l.MaxItems) resp.NextPageToken = resp.Records[^1].Key;
        return resp;
    }

    private sealed class ByteOrder : IComparer<byte[]>
    {
        public static readonly ByteOrder Instance = new();

        public int Compare(byte[]? a, byte[]? b) => a.AsSpan().SequenceCompareTo(b);
    }

    private sealed class Reader<T>(IEnumerable<T> items) : IAsyncStreamReader<T>
    {
        private readonly IEnumerator<T> _e = items.GetEnumerator();

        public T Current => _e.Current;

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(_e.MoveNext());
    }
}
