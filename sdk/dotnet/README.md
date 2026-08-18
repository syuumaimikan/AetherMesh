# AetherMesh.Client (.NET)

C# client for [AetherMesh](../../README.md): publish data once, run tasks —
including WebAssembly modules — across a mesh of machines.

.NET 8+. No package references: the protocol is four bytes of big-endian length
and one JSON object, which `System.Text.Json` and a socket already cover.

## Build it

```bash
dotnet build sdk/dotnet/AetherMesh
```

Or reference the project directly:

```xml
<ProjectReference Include="path/to/sdk/dotnet/AetherMesh/AetherMesh.csproj" />
```

## Use it

```csharp
using AetherMesh;

await using var mesh = await MeshClient.ConnectAsync(new MeshOptions { Port = 7100 });

foreach (var node in await mesh.NodesAsync())
{
    Console.WriteLine($"{node.Hostname} {string.Join(" ", node.Labels)}");
}

// Published once; the mesh moves it only to nodes that actually need it.
var data = await mesh.PublishAsync(await File.ReadAllBytesAsync("input.bin"));

var result = await mesh.RunAsync("hash", "seed"u8.ToArray(), inputs: [data.DataId]);
Console.WriteLine($"{Convert.ToHexString(result.Output)} on {result.NodeId} in {result.DurationMs:F1} ms");
```

### WebAssembly tasks

```csharp
var module = await mesh.PublishFileAsync("uppercase.wasm");
var result = await mesh.RunWasmAsync(module.DataId, "hello"u8.ToArray());
Encoding.UTF8.GetString(result.Output);  // "HELLO"
```

Building a module from Rust, TypeScript, or Go:
[`docs/wasm-tasks.md`](../../docs/wasm-tasks.md).

### Restricting where a task runs

```csharp
await mesh.RunAsync("hash", payload, constraints: ["kind=gpu", "region!=us-east"]);
```

`key=value`, `key!=value`, or a bare `key` for "has this label at all". Nodes
declare theirs with `aether-agent --label kind=gpu`. A task no node satisfies is
refused, not relocated.

### Authentication and TLS

```csharp
var mesh = await MeshClient.ConnectAsync(new MeshOptions
{
    Host = "mesh.example.com",
    Port = 7100,
    Token = Environment.GetEnvironmentVariable("AETHERMESH_TOKEN"),
    TlsCaPath = "cert.pem",
});
```

Naming the CA is deliberate: a self-signed controller is the normal case, and
the machine trust store will not have it.

## API

| Member | Returns |
|---|---|
| `MeshClient.ConnectAsync(options, ct)` | a connected client (`IAsyncDisposable`) |
| `PublishAsync(bytes)` / `PublishFileAsync(path)` | `Published(DataId, SizeBytes)` |
| `RunAsync(kind, payload, inputs, constraints, ct)` | `TaskResult` — built-in `echo`, `hash`, `cpu` |
| `RunWasmAsync(moduleId, payload, inputs, constraints, ct)` | `TaskResult` |
| `RunAsync(kind, payload, inputs, constraints, priority, ct)` | `TaskResult`, saying how urgently it wants a node |
| `WorkflowAsync(steps, run, ct)` | `WorkflowResult` — steps that depend on each other; `run` resumes |
| `NodesAsync(ct)` | `IReadOnlyList<NodeSummary>` — labels, `DatasetsHeld`, `Connected`, … |
| `RecentAsync(limit, ct)` | `IReadOnlyList<FinishedTask>` — what the *mesh* finished lately |
| `StatsAsync(ct)` | traffic, counters, queue, as the controller sent them |

[`Examples/CheckAll`](Examples/CheckAll) exercises all of it against a running
mesh, which is how the table above is kept honest.

A `TaskResult` carries `TaskId`, `NodeId`, `Success`, `Output`, `DurationMs`,
and `Error`. A task that ran and failed comes back with `Success == false` and an
`Error`; only transport and protocol problems throw `MeshException`.

## Threading

One `MeshClient` is not safe for concurrent use — replies are matched to
requests by order. Open one per worker.

## Example

```bash
dotnet run --project sdk/dotnet/Examples/Hash -- --tasks 5 --mib 8
```

```
1 node(s): syuum [kind=cpu]
published 8 MiB in 106 ms as 44ecb3af551ef7e0…
  task   0: ae05af74f132f9aa… on 27c8e173 in    2.3 ms
  task   1: ca38d0a373474e24… on 27c8e173 in    2.8 ms
  task   4: f5e159ddf24d49ca… on 27c8e173 in    1.8 ms
```

The 8 MiB crossed the wire once. Every task after the first found it already
there, which is the whole point.
