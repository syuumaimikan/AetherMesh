/**
 * AetherMesh client for TypeScript and JavaScript.
 *
 * The wire format is deliberately boring: a 4-byte big-endian length followed
 * by one JSON object, in both directions. That is all this file implements —
 * so the same 200 lines port to Python, Go, or anything with a socket.
 *
 * ```ts
 * const mesh = await AetherMesh.connect({ host: "127.0.0.1", port: 7100 });
 * const module = await mesh.publish(await readFile("task.wasm"));
 * const result = await mesh.runWasm(module.dataId, new TextEncoder().encode("hi"));
 * console.log(new TextDecoder().decode(result.output));
 * await mesh.close();
 * ```
 */

import { connect as tcpConnect, type Socket } from "node:net";
import { connect as tlsConnect } from "node:tls";
import { readFile } from "node:fs/promises";

/** Where the controller's client API is, and how to authenticate to it. */
export interface ConnectOptions {
  host?: string;
  port?: number;
  /** Shared secret, when the controller requires one. */
  token?: string;
  /** Path to the controller's certificate. Setting it switches to TLS. */
  tlsCaPath?: string;
  /** Name the certificate was issued for. Defaults to `host`. */
  tlsServerName?: string;
  /** Milliseconds to wait for one response. Default 120 s. */
  timeoutMs?: number;
}

/** A dataset the controller now holds. */
export interface Published {
  dataId: string;
  sizeBytes: number;
}

/** What a task produced. */
export interface TaskResult {
  taskId: string;
  nodeId: string;
  success: boolean;
  output: Uint8Array;
  durationMs: number;
  error?: string;
}

/** One node in the mesh. */
export interface NodeSummary {
  nodeId: string;
  hostname: string;
  cpuCores: number;
  cpuUsage: number;
  memoryUsage: number;
  /** What the node claims to be: `{ gpu: "true", region: "eu-west" }`. */
  labels: Record<string, string>;
}

/** The controller answered with an error, or the connection failed. */
export class AetherMeshError extends Error {}

type Frame = Record<string, unknown>;

interface Pending {
  resolve: (frame: Frame) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** A connection to an AetherMesh controller. */
export class AetherMesh {
  #socket: Socket;
  #buffer: Buffer = Buffer.alloc(0);
  /** Requests are answered in order, so a queue is enough to match them up. */
  #pending: Pending[] = [];
  #timeoutMs: number;
  #closed = false;

  private constructor(socket: Socket, timeoutMs: number) {
    this.#socket = socket;
    this.#timeoutMs = timeoutMs;

    socket.on("data", (chunk: Buffer) => this.#onData(chunk));
    socket.on("error", (error: Error) => this.#failAll(error));
    socket.on("close", () => {
      if (!this.#closed)
        this.#failAll(new AetherMeshError("connection closed"));
    });
  }

  /** Opens a connection and completes the handshake. */
  static async connect(options: ConnectOptions = {}): Promise<AetherMesh> {
    const host = options.host ?? "127.0.0.1";
    const port = options.port ?? 7100;
    const timeoutMs = options.timeoutMs ?? 120_000;

    const socket = options.tlsCaPath
      ? tlsConnect({
          host,
          port,
          ca: [await readFile(options.tlsCaPath)],
          servername: options.tlsServerName ?? host,
        })
      : tcpConnect({ host, port });

    await new Promise<void>((resolve, reject) => {
      const event = options.tlsCaPath ? "secureConnect" : "connect";
      socket.once(event, () => resolve());
      socket.once("error", reject);
    });

    const mesh = new AetherMesh(socket, timeoutMs);
    const welcome = await mesh.#request({
      type: "hello",
      token: options.token ?? null,
    });
    if (welcome.type !== "welcome") {
      mesh.close();
      throw new AetherMeshError(String(welcome.message ?? "handshake refused"));
    }
    return mesh;
  }

  /** Stores data on the controller. Identical bytes yield the same id. */
  async publish(data: Uint8Array): Promise<Published> {
    const frame = await this.#request({
      type: "publish",
      data: Buffer.from(data).toString("base64"),
    });
    this.#expect(frame, "published");
    return {
      dataId: String(frame.data_id),
      sizeBytes: Number(frame.size_bytes),
    };
  }

  /** Publishes a file, e.g. a compiled `.wasm` module. */
  async publishFile(path: string): Promise<Published> {
    return this.publish(await readFile(path));
  }

  /**
   * Runs a built-in task: `echo`, `hash`, or `cpu`.
   *
   * `inputs` are ids from {@link publish}; the mesh moves them to the chosen
   * node only if that node does not already hold them.
   *
   * `constraints` say where the task may run at all — `"gpu=true"`,
   * `"region!=us-east"`, `"nvme"` (label present). If no node qualifies the
   * task fails rather than running somewhere it was not allowed.
   */
  async run(
    kind: string,
    payload: Uint8Array = new Uint8Array(),
    inputs: string[] = [],
    constraints: string[] = [],
  ): Promise<TaskResult> {
    return this.#submit({ kind, payload, inputs, constraints });
  }

  /** Runs a WebAssembly module previously published with {@link publish}. */
  async runWasm(
    moduleId: string,
    payload: Uint8Array = new Uint8Array(),
    inputs: string[] = [],
    constraints: string[] = [],
  ): Promise<TaskResult> {
    return this.#submit({
      kind: "wasm",
      payload,
      inputs,
      constraints,
      module: moduleId,
    });
  }

  /** Lists the nodes currently in the mesh. */
  async nodes(): Promise<NodeSummary[]> {
    const frame = await this.#request({ type: "nodes" });
    this.#expect(frame, "nodes");
    return (frame.nodes as Frame[]).map((node) => ({
      nodeId: String(node.node_id),
      hostname: String(node.hostname),
      cpuCores: Number(node.cpu_cores),
      cpuUsage: Number(node.cpu_usage),
      memoryUsage: Number(node.memory_usage),
      labels: (node.labels as Record<string, string>) ?? {},
    }));
  }

  /** Closes the connection. */
  close(): void {
    this.#closed = true;
    this.#socket.end();
  }

  async #submit(task: {
    kind: string;
    payload: Uint8Array;
    inputs: string[];
    constraints: string[];
    module?: string;
  }): Promise<TaskResult> {
    const frame = await this.#request({
      type: "submit",
      kind: task.kind,
      payload: Buffer.from(task.payload).toString("base64"),
      inputs: task.inputs,
      constraints: task.constraints,
      module: task.module ?? null,
    });
    this.#expect(frame, "result");

    return {
      taskId: String(frame.task_id),
      nodeId: String(frame.node_id),
      success: Boolean(frame.success),
      output: new Uint8Array(Buffer.from(String(frame.output), "base64")),
      durationMs: Number(frame.duration_ms),
      error:
        frame.error === undefined || frame.error === null
          ? undefined
          : String(frame.error),
    };
  }

  /** Throws when the controller answered with something else, e.g. an error. */
  #expect(frame: Frame, type: string): void {
    if (frame.type === type) return;
    throw new AetherMeshError(
      String(frame.message ?? `expected ${type}, got ${frame.type}`),
    );
  }

  #request(request: Frame): Promise<Frame> {
    if (this.#closed) {
      return Promise.reject(new AetherMeshError("connection is closed"));
    }

    return new Promise<Frame>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending = this.#pending.filter((entry) => entry.timer !== timer);
        reject(new AetherMeshError(`no response within ${this.#timeoutMs} ms`));
      }, this.#timeoutMs);

      this.#pending.push({ resolve, reject, timer });

      const payload = Buffer.from(JSON.stringify(request), "utf8");
      const header = Buffer.alloc(4);
      header.writeUInt32BE(payload.length);
      this.#socket.write(Buffer.concat([header, payload]));
    });
  }

  #onData(chunk: Buffer): void {
    this.#buffer = Buffer.concat([this.#buffer, chunk]);

    // A frame is only complete once its declared length has arrived.
    while (this.#buffer.length >= 4) {
      const length = this.#buffer.readUInt32BE(0);
      if (this.#buffer.length < 4 + length) return;

      const payload = this.#buffer.subarray(4, 4 + length);
      this.#buffer = this.#buffer.subarray(4 + length);

      const waiter = this.#pending.shift();
      if (!waiter) continue;
      clearTimeout(waiter.timer);

      try {
        waiter.resolve(JSON.parse(payload.toString("utf8")) as Frame);
      } catch (error) {
        waiter.reject(
          new AetherMeshError(`controller sent invalid JSON: ${String(error)}`),
        );
      }
    }
  }

  #failAll(error: Error): void {
    const waiters = this.#pending;
    this.#pending = [];
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(
        error instanceof AetherMeshError
          ? error
          : new AetherMeshError(error.message),
      );
    }
  }
}

export default AetherMesh;
