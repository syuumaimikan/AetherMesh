using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace AetherMesh;

/// <summary>The controller answered with an error, or the connection failed.</summary>
public sealed class MeshException : Exception
{
    /// <summary>Creates the exception with the message the controller gave.</summary>
    public MeshException(string message) : base(message) { }
}

/// <summary>A dataset the controller now holds.</summary>
/// <remarks>
/// <c>DataId</c> is a content address: identical bytes always yield the same one.
/// </remarks>
public readonly record struct Published(string DataId, long SizeBytes);

/// <summary>What a task produced.</summary>
/// <remarks>
/// A task that ran and failed has <see cref="Success"/> false and an
/// <see cref="Error"/>; only transport and protocol problems throw.
/// </remarks>
public readonly record struct TaskResult(
    string TaskId,
    string NodeId,
    bool Success,
    byte[] Output,
    double DurationMs,
    string? Error);

/// <summary>One node in the mesh.</summary>
/// <remarks><c>Labels</c> is what the node claims to be, e.g. <c>gpu=true</c>.</remarks>
public readonly record struct NodeSummary(
    string NodeId,
    string Hostname,
    int CpuCores,
    double CpuUsage,
    double MemoryUsage,
    IReadOnlyDictionary<string, string> Labels,
    string Address,
    int DatasetsHeld,
    long BytesHeld,
    bool Connected);

/// <summary>Who waits once more work has arrived than there are nodes.</summary>
/// <remarks>
/// Changes nothing on an idle mesh: a queue nobody is waiting in has no order
/// worth arguing about.
/// </remarks>
public enum Priority
{
    /// <summary>Ahead of everything else waiting.</summary>
    Critical,

    /// <summary>Ahead of ordinary work: a person is waiting on it.</summary>
    High,

    /// <summary>The default.</summary>
    Normal,

    /// <summary>Behind ordinary work, but not behind it forever.</summary>
    Low,

    /// <summary>Whenever nothing else wants the node.</summary>
    Background,
}

/// <summary>One step of a workflow.</summary>
/// <remarks>
/// <c>DependsOn</c> holds indices into the list of steps. Every dependency's
/// output becomes an input of the step waiting for it, so a step reads what the
/// steps before it produced — and, because the mesh knows which node holds that
/// output, reads it without moving it.
/// </remarks>
public sealed record Step(
    string Kind,
    ReadOnlyMemory<byte> Payload = default,
    IReadOnlyList<int>? DependsOn = null,
    IReadOnlyList<string>? Inputs = null,
    IReadOnlyList<string>? Constraints = null,
    string? Module = null);

/// <summary>What one step of a workflow did.</summary>
/// <remarks>
/// <c>Step</c> is its index in the submitted workflow, not its position in the
/// reply — those differ as soon as any step is skipped or resumed.
/// </remarks>
public readonly record struct StepOutcome(
    int Step,
    string NodeId,
    bool Success,
    byte[] Output,
    double DurationMs,
    string? Error);

/// <summary>What a workflow produced.</summary>
public readonly record struct WorkflowResult(
    IReadOnlyList<StepOutcome> Steps,
    IReadOnlyList<int> Skipped,
    IReadOnlyList<int> Resumed,
    bool Success);

/// <summary>One task that finished anywhere in the mesh.</summary>
public readonly record struct FinishedTask(
    string TaskId,
    string Kind,
    string NodeId,
    bool Success,
    double DurationMs,
    long OutputBytes,
    string Preview,
    double SecondsAgo);

/// <summary>Where the controller is and how to authenticate to it.</summary>
public sealed class MeshOptions
{
    /// <summary>Controller hostname or address.</summary>
    public string Host { get; init; } = "127.0.0.1";

    /// <summary>Port the client API listens on.</summary>
    public int Port { get; init; } = 7100;

    /// <summary>Shared secret, when the controller requires one.</summary>
    public string? Token { get; init; }

    /// <summary>The controller's certificate. Setting it switches to TLS.</summary>
    public string? TlsCaPath { get; init; }

    /// <summary>Name the certificate was issued for. Defaults to <see cref="Host"/>.</summary>
    public string? TlsServerName { get; init; }

    /// <summary>How long to wait for one response.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(2);
}

/// <summary>
/// A connection to an AetherMesh controller.
/// </summary>
/// <remarks>
/// <para>
/// The wire format is four bytes of big-endian length followed by one JSON
/// object, in both directions. That is the whole protocol, which is why this
/// file needs nothing outside the base class library.
/// </para>
/// <para>
/// One instance is not safe for concurrent use: replies are matched to
/// requests by order. Open one connection per worker.
/// </para>
/// </remarks>
public sealed class MeshClient : IAsyncDisposable, IDisposable
{
    /// <summary>Refuses to allocate more than this for one response.</summary>
    public const int MaxFrameBytes = 256 * 1024 * 1024;

    private readonly Stream _stream;
    private readonly Socket _socket;

    private MeshClient(Socket socket, Stream stream)
    {
        _socket = socket;
        _stream = stream;
    }

    /// <summary>Opens a connection and completes the handshake.</summary>
    public static async Task<MeshClient> ConnectAsync(
        MeshOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new MeshOptions();

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        Stream stream;
        try
        {
            await socket.ConnectAsync(options.Host, options.Port, cancellationToken)
                .ConfigureAwait(false);
            stream = new NetworkStream(socket, ownsSocket: false)
            {
                ReadTimeout = (int)options.Timeout.TotalMilliseconds,
                WriteTimeout = (int)options.Timeout.TotalMilliseconds,
            };

            if (options.TlsCaPath is { } caPath)
            {
                stream = await AuthenticateAsync(stream, options, caPath, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        var client = new MeshClient(socket, stream);
        var welcome = await client.RequestAsync(
            new Dictionary<string, object?> { ["type"] = "hello", ["token"] = options.Token },
            cancellationToken).ConfigureAwait(false);

        if (Text(welcome, "type") != "welcome")
        {
            client.Dispose();
            throw new MeshException(Text(welcome, "message") ?? "handshake refused");
        }
        return client;
    }

    /// <summary>Stores data on the controller. Identical bytes yield the same id.</summary>
    public async Task<Published> PublishAsync(
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        var frame = await RequestAsync(new Dictionary<string, object?>
        {
            ["type"] = "publish",
            ["data"] = Convert.ToBase64String(data.Span),
        }, cancellationToken).ConfigureAwait(false);

        Expect(frame, "published");
        return new Published(Text(frame, "data_id") ?? "", Number(frame, "size_bytes") is { } n ? (long)n : 0);
    }

    /// <summary>Publishes a file, e.g. a compiled <c>.wasm</c> module.</summary>
    public async Task<Published> PublishFileAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        return await PublishAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs a built-in task: <c>echo</c>, <c>hash</c>, or <c>cpu</c>.</summary>
    /// <remarks>
    /// <para>
    /// <c>inputs</c> are ids from <see cref="PublishAsync"/>. The mesh moves
    /// them to the chosen node only if that node does not already hold them.
    /// </para>
    /// <para>
    /// <c>constraints</c> say where the task may run at all: <c>gpu=true</c>,
    /// <c>region!=us-east</c>, or a bare <c>nvme</c> for a label that only has
    /// to be present. A task no node satisfies is refused rather than placed
    /// somewhere it was not allowed.
    /// </para>
    /// </remarks>
    public Task<TaskResult> RunAsync(
        string kind,
        ReadOnlyMemory<byte> payload = default,
        IEnumerable<string>? inputs = null,
        IEnumerable<string>? constraints = null,
        Priority? priority = null,
        CancellationToken cancellationToken = default)
        => SubmitAsync(kind, payload, inputs, constraints, module: null, priority, cancellationToken);

    /// <summary>Runs several tasks, each after the ones it depends on.</summary>
    /// <remarks>
    /// <para>
    /// A non-null <paramref name="run"/> names the run, so submitting the same
    /// workflow again resumes it rather than repeating it: steps that already
    /// finished are skipped when their output is still on a node. Needs a
    /// controller started with <c>checkpoint_path</c>.
    /// </para>
    /// <para>
    /// Reusing a name for a <em>different</em> workflow throws rather than
    /// resuming. Skipping step 3 because another graph's step 3 finished is the
    /// one failure here that answers confidently and wrongly.
    /// </para>
    /// </remarks>
    public async Task<WorkflowResult> WorkflowAsync(
        IEnumerable<Step> steps,
        string? run = null,
        CancellationToken cancellationToken = default)
    {
        var encoded = steps.Select(step => new Dictionary<string, object?>
        {
            ["kind"] = step.Kind,
            ["payload"] = Convert.ToBase64String(step.Payload.Span),
            ["depends_on"] = step.DependsOn?.ToArray() ?? Array.Empty<int>(),
            ["inputs"] = step.Inputs?.ToArray() ?? Array.Empty<string>(),
            ["constraints"] = step.Constraints?.ToArray() ?? Array.Empty<string>(),
            ["module"] = step.Module,
        }).ToArray();

        var request = new Dictionary<string, object?>
        {
            ["type"] = "workflow",
            ["steps"] = encoded,
        };
        if (run is not null)
        {
            request["run"] = run;
        }

        var frame = await RequestAsync(request, cancellationToken).ConfigureAwait(false);
        Expect(frame, "workflow");

        var outcomes = new List<StepOutcome>();
        if (frame.TryGetProperty("steps", out var array) && array.ValueKind == JsonValueKind.Array)
        {
            foreach (var step in array.EnumerateArray())
            {
                outcomes.Add(new StepOutcome(
                    (int)(Number(step, "step") ?? 0),
                    Text(step, "node_id") ?? "",
                    step.TryGetProperty("success", out var ok) && ok.ValueKind == JsonValueKind.True,
                    Convert.FromBase64String(Text(step, "output") ?? ""),
                    Number(step, "duration_ms") ?? 0,
                    Text(step, "error")));
            }
        }

        return new WorkflowResult(
            outcomes,
            Integers(frame, "skipped"),
            Integers(frame, "resumed"),
            frame.TryGetProperty("success", out var success) && success.ValueKind == JsonValueKind.True);
    }

    /// <summary>The last few tasks that finished anywhere in the mesh.</summary>
    /// <remarks>
    /// Not only the ones this connection submitted: a task somebody else ran is
    /// exactly the interesting case. The preview is the front of the output,
    /// not the output — results stay on the node that produced them.
    /// </remarks>
    public async Task<IReadOnlyList<FinishedTask>> RecentAsync(
        int limit = 20,
        CancellationToken cancellationToken = default)
    {
        var frame = await RequestAsync(
            new Dictionary<string, object?> { ["type"] = "recent", ["limit"] = limit },
            cancellationToken).ConfigureAwait(false);
        Expect(frame, "recent");

        var tasks = new List<FinishedTask>();
        if (!frame.TryGetProperty("tasks", out var array) || array.ValueKind != JsonValueKind.Array)
        {
            return tasks;
        }

        foreach (var task in array.EnumerateArray())
        {
            tasks.Add(new FinishedTask(
                Text(task, "task_id") ?? "",
                Text(task, "kind") ?? "",
                Text(task, "node_id") ?? "",
                task.TryGetProperty("success", out var ok) && ok.ValueKind == JsonValueKind.True,
                Number(task, "duration_ms") ?? 0,
                (long)(Number(task, "output_bytes") ?? 0),
                Text(task, "preview") ?? "",
                Number(task, "seconds_ago") ?? 0));
        }
        return tasks;
    }

    /// <summary>What the mesh has moved, saved, run and queued.</summary>
    /// <remarks>
    /// Returned as the controller sent it rather than as a record: this is a
    /// dashboard feed that grows fields, and a client receiving one it does not
    /// know about should see it rather than lose it.
    /// </remarks>
    public async Task<JsonElement> StatsAsync(CancellationToken cancellationToken = default)
    {
        var frame = await RequestAsync(
            new Dictionary<string, object?> { ["type"] = "stats" },
            cancellationToken).ConfigureAwait(false);
        Expect(frame, "stats");
        return frame;
    }

    /// <summary>Runs a WebAssembly module previously published.</summary>
    public Task<TaskResult> RunWasmAsync(
        string moduleId,
        ReadOnlyMemory<byte> payload = default,
        IEnumerable<string>? inputs = null,
        IEnumerable<string>? constraints = null,
        Priority? priority = null,
        CancellationToken cancellationToken = default)
        => SubmitAsync("wasm", payload, inputs, constraints, moduleId, priority, cancellationToken);

    /// <summary>Lists the nodes currently in the mesh.</summary>
    public async Task<IReadOnlyList<NodeSummary>> NodesAsync(
        CancellationToken cancellationToken = default)
    {
        var frame = await RequestAsync(
            new Dictionary<string, object?> { ["type"] = "nodes" },
            cancellationToken).ConfigureAwait(false);
        Expect(frame, "nodes");

        var nodes = new List<NodeSummary>();
        if (!frame.TryGetProperty("nodes", out var array) || array.ValueKind != JsonValueKind.Array)
        {
            return nodes;
        }

        foreach (var node in array.EnumerateArray())
        {
            var labels = new Dictionary<string, string>();
            if (node.TryGetProperty("labels", out var raw) && raw.ValueKind == JsonValueKind.Object)
            {
                foreach (var label in raw.EnumerateObject())
                {
                    labels[label.Name] = label.Value.GetString() ?? "";
                }
            }

            nodes.Add(new NodeSummary(
                Text(node, "node_id") ?? "",
                Text(node, "hostname") ?? "",
                (int)(Number(node, "cpu_cores") ?? 0),
                Number(node, "cpu_usage") ?? 0,
                Number(node, "memory_usage") ?? 0,
                labels,
                Text(node, "address") ?? "",
                (int)(Number(node, "datasets_held") ?? 0),
                (long)(Number(node, "bytes_held") ?? 0),
                !node.TryGetProperty("connected", out var connected)
                    || connected.ValueKind != JsonValueKind.False));
        }
        return nodes;
    }

    /// <summary>Closes the connection.</summary>
    public void Dispose()
    {
        _stream.Dispose();
        _socket.Dispose();
    }

    /// <summary>Closes the connection.</summary>
    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _socket.Dispose();
    }

    private async Task<TaskResult> SubmitAsync(
        string kind,
        ReadOnlyMemory<byte> payload,
        IEnumerable<string>? inputs,
        IEnumerable<string>? constraints,
        string? module,
        Priority? priority,
        CancellationToken cancellationToken)
    {
        var frame = await RequestAsync(new Dictionary<string, object?>
        {
            ["type"] = "submit",
            ["kind"] = kind,
            ["payload"] = Convert.ToBase64String(payload.Span),
            ["inputs"] = inputs?.ToArray() ?? Array.Empty<string>(),
            ["constraints"] = constraints?.ToArray() ?? Array.Empty<string>(),
            ["module"] = module,
            ["priority"] = priority?.ToString().ToLowerInvariant(),
        }, cancellationToken).ConfigureAwait(false);

        Expect(frame, "result");
        return new TaskResult(
            Text(frame, "task_id") ?? "",
            Text(frame, "node_id") ?? "",
            frame.TryGetProperty("success", out var ok) && ok.ValueKind == JsonValueKind.True,
            Convert.FromBase64String(Text(frame, "output") ?? ""),
            Number(frame, "duration_ms") ?? 0,
            Text(frame, "error"));
    }

    private async Task<JsonElement> RequestAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(request);
        var header = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(header, (uint)payload.Length);

        await _stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);

        await ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadUInt32BigEndian(header);
        if (length > MaxFrameBytes)
        {
            throw new MeshException($"controller announced a {length} byte frame");
        }

        var body = new byte[length];
        await ReadExactlyAsync(body, cancellationToken).ConfigureAwait(false);

        try
        {
            return JsonDocument.Parse(body).RootElement.Clone();
        }
        catch (JsonException error)
        {
            throw new MeshException($"controller sent invalid JSON: {error.Message}");
        }
    }

    private async Task ReadExactlyAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var filled = 0;
        while (filled < buffer.Length)
        {
            var read = await _stream.ReadAsync(buffer[filled..], cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
            {
                throw new MeshException("connection closed by the controller");
            }
            filled += read;
        }
    }

    private static async Task<Stream> AuthenticateAsync(
        Stream stream,
        MeshOptions options,
        string caPath,
        CancellationToken cancellationToken)
    {
        // A self-signed controller is the normal case, so the CA to trust is
        // named explicitly rather than assumed to be in the machine store.
        var authority = new X509Certificate2Collection();
        authority.ImportFromPemFile(caPath);

        var tls = new SslStream(stream, leaveInnerStreamOpen: false, (_, certificate, chain, errors) =>
        {
            if (errors == SslPolicyErrors.None) return true;
            if (certificate is null || chain is null) return false;
            if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0) return false;

            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.AddRange(authority);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            return chain.Build(new X509Certificate2(certificate));
        });

        await tls.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions
            {
                TargetHost = options.TlsServerName ?? options.Host,
            },
            cancellationToken).ConfigureAwait(false);
        return tls;
    }

    private static void Expect(JsonElement frame, string type)
    {
        if (Text(frame, "type") == type) return;
        throw new MeshException(
            Text(frame, "message") ?? $"expected {type}, got {Text(frame, "type")}");
    }

    /// <summary>Reads a JSON array of numbers, which is how step indices arrive.</summary>
    private static IReadOnlyList<int> Integers(JsonElement frame, string name)
    {
        var out_ = new List<int>();
        if (frame.TryGetProperty(name, out var array) && array.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in array.EnumerateArray())
            {
                if (entry.TryGetInt32(out var value))
                {
                    out_.Add(value);
                }
            }
        }
        return out_;
    }

    private static string? Text(JsonElement frame, string name) =>
        frame.ValueKind == JsonValueKind.Object
        && frame.TryGetProperty(name, out var value)
        && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static double? Number(JsonElement frame, string name) =>
        frame.ValueKind == JsonValueKind.Object
        && frame.TryGetProperty(name, out var value)
        && value.ValueKind == JsonValueKind.Number
            ? value.GetDouble()
            : null;
}
