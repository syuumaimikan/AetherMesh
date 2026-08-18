# aethermesh (Java)

Java client for [AetherMesh](../../README.md): publish data once, run tasks —
including WebAssembly modules — across a mesh of machines.

Java 17+. **No dependencies**, not even a JSON library: the protocol is four
bytes of big-endian length and one JSON object, and the frames it exchanges are
a small enough grammar to parse in [`Json.java`](src/main/java/dev/aethermesh/Json.java)
without putting a dependency-resolution problem between you and your first task.

## Build it

Two source files, so a build tool is optional:

```bash
javac -d out sdk/java/src/main/java/dev/aethermesh/*.java
```

Or drop `src/main/java` into an existing Maven or Gradle project — there is
nothing to add to `pom.xml` beyond the source directory.

## Use it

```java
import dev.aethermesh.AetherMesh;
import java.nio.file.Path;
import java.util.List;

try (AetherMesh mesh = AetherMesh.connect(new AetherMesh.Options().port(7100))) {
    for (var node : mesh.nodes()) {
        System.out.println(node.hostname() + " " + node.labels());
    }

    // Published once; the mesh moves it only to nodes that actually need it.
    var data = mesh.publish(Files.readAllBytes(Path.of("input.bin")));

    var result = mesh.run("hash", "seed".getBytes(), List.of(data.dataId()), List.of());
    System.out.printf("%s on %s in %.1f ms%n",
        HexFormat.of().formatHex(result.output()), result.nodeId(), result.durationMs());
}
```

### WebAssembly tasks

```java
var module = mesh.publishFile(Path.of("uppercase.wasm"));
var result = mesh.runWasm(module.dataId(), "hello".getBytes());
new String(result.output());  // "HELLO"
```

Building a module from Rust, TypeScript, or Go:
[`docs/wasm-tasks.md`](../../docs/wasm-tasks.md).

### Restricting where a task runs

```java
mesh.run("hash", payload, List.of(), List.of("kind=gpu", "region!=us-east"));
```

`key=value`, `key!=value`, or a bare `key` for "has this label at all". Nodes
declare theirs with `aether-agent --label kind=gpu`. A task no node satisfies is
refused, not relocated.

### Authentication and TLS

```java
var options = new AetherMesh.Options()
    .host("mesh.example.com")
    .port(7100)
    .token(System.getenv("AETHERMESH_TOKEN"))
    .tlsCaPath(Path.of("cert.pem"));
```

Naming the CA is deliberate: a self-signed controller is the normal case, and
the JDK's default trust store will not have it.

## API

| Method | Returns |
|---|---|
| `AetherMesh.connect(options)` | a connected client (`AutoCloseable`) |
| `mesh.publish(byte[])` / `mesh.publishFile(path)` | `Published(dataId, sizeBytes)` |
| `mesh.run(kind, payload[, inputs, constraints])` | `TaskResult` — built-in `echo`, `hash`, `cpu` |
| `mesh.runWasm(moduleId, payload[, inputs, constraints])` | `TaskResult` |
| `mesh.run(kind, payload, priority, inputs, constraints)` | `TaskResult`, saying how urgently it wants a node |
| `mesh.workflow(steps[, run])` | `WorkflowResult` — steps that depend on each other; `run` resumes |
| `mesh.nodes()` | `List<NodeSummary>` — labels, `datasetsHeld`, `connected`, … |
| `mesh.recent(limit)` | `List<FinishedTask>` — what the *mesh* finished lately |
| `mesh.stats()` | `Map<String, Object>` — traffic, counters, queue |
| `mesh.close()` | — |

[`examples/CheckAll.java`](examples/CheckAll.java) exercises all of it against a
running mesh, which is how the table above is kept honest.

A `TaskResult` carries `taskId`, `nodeId`, `success`, `output`, `durationMs`,
and `error`. A task that ran and failed comes back with `success() == false` and
an `error()`; only transport and protocol problems throw `MeshException`.

## Threading

One connection is not safe for concurrent use — replies are matched to requests
by order. Open one per thread.

## Example

```bash
javac -d out sdk/java/src/main/java/dev/aethermesh/*.java sdk/java/examples/HashExample.java
java -cp out HashExample --tasks 5 --mib 8
```

```
1 node(s): [syuum {kind=cpu}]
published 8 MiB in 118 ms as ae116eef5d68b0e3…
  task   0: dd6f4a1f3434dad0… on 27c8e173 in    1.6 ms
  task   1: 0fa76d740e7d498f… on 27c8e173 in    1.5 ms
  task   4: dca85d50a8c97251… on 27c8e173 in    1.4 ms
```

The 8 MiB crossed the wire once. Every task after the first found it already
there, which is the whole point.
