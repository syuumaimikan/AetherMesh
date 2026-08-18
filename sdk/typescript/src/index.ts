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
  address: string;
  latencyMs?: number;
  bandwidthBytesPerSec?: number;
  /**
   * Datasets this node already holds, and their total size. Work reading them
   * costs no transfer, which is the decision the scheduler makes every time.
   */
  datasetsHeld: number;
  bytesHeld: number;
  /**
   * Registered is not the same as reachable: a node keeps its registration
   * until its heartbeat times out, because one late heartbeat is not a death.
   */
  connected: boolean;
}

/**
 * One step of a workflow.
 *
 * `dependsOn` holds indices into the list of steps. Every dependency's output
 * becomes an input of the step waiting for it, so a step reads what the steps
 * before it produced — and, because the mesh knows which node holds that
 * output, reads it without moving it.
 */
export interface Step {
  kind: string;
  payload?: Uint8Array;
  dependsOn?: number[];
  inputs?: string[];
  constraints?: string[];
  module?: string;
}

/** What one step of a workflow did. */
export interface StepOutcome {
  /**
   * Which step of the submitted workflow this is — not its position in the
   * reply, which differs as soon as any step is skipped or resumed.
   */
  step: number;
  nodeId: string;
  success: boolean;
  output: Uint8Array;
  durationMs: number;
  error?: string;
}

/** What a workflow produced. */
export interface WorkflowResult {
  steps: StepOutcome[];
  /** Steps never attempted because something they depend on failed. */
  skipped: number[];
  /**
   * Steps an earlier run of the same name had already finished. Only ever
   * non-empty for a named run against a controller with a checkpoint file.
   */
  resumed: number[];
  success: boolean;
}

/** One task that finished anywhere in the mesh. */
export interface FinishedTask {
  taskId: string;
  kind: string;
  nodeId: string;
  success: boolean;
  durationMs: number;
  /** Size of the whole output, of which `preview` is the front. */
  outputBytes: number;
  /** The first bytes of the output, with anything unprintable replaced. */
  preview: string;
  secondsAgo: number;
}

/**
 * Who waits once more work has arrived than there are nodes.
 *
 * Changes nothing on an idle mesh: a queue nobody is waiting in has no order
 * worth arguing about.
 */
export type Priority =
  | "critical"
  | "high"
  | "normal"
  | "low"
  | "background";

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
    priority?: Priority,
    timeoutMs?: number,
  ): Promise<TaskResult> {
    return this.#submit({ kind, payload, inputs, constraints, priority, timeoutMs });
  }

  /** Runs a WebAssembly module previously published with {@link publish}. */
  async runWasm(
    moduleId: string,
    payload: Uint8Array = new Uint8Array(),
    inputs: string[] = [],
    constraints: string[] = [],
    priority?: Priority,
    timeoutMs?: number,
  ): Promise<TaskResult> {
    return this.#submit({
      kind: "wasm",
      payload,
      inputs,
      constraints,
      module: moduleId,
      priority,
      timeoutMs,
    });
  }

  /**
   * Runs several tasks, each after the ones it depends on.
   *
   * `run` names the run, so submitting the same workflow again resumes it
   * rather than repeating it: steps that already finished are skipped,
   * provided their output is still on a node. It needs a controller started
   * with `checkpoint_path`; without one the name is accepted and the workflow
   * runs from the start.
   *
   * Reusing a name for a *different* workflow throws rather than resuming.
   * Skipping step 3 on the strength of another graph's step 3 is the one
   * failure here that would produce a confident wrong answer.
   */
  async workflow(steps: Step[], run?: string): Promise<WorkflowResult> {
    const request: Frame = {
      type: "workflow",
      steps: steps.map((step) => ({
        kind: step.kind,
        payload: Buffer.from(step.payload ?? new Uint8Array()).toString(
          "base64",
        ),
        depends_on: step.dependsOn ?? [],
        inputs: step.inputs ?? [],
        constraints: step.constraints ?? [],
        module: step.module ?? null,
      })),
    };
    if (run !== undefined) {
      request.run = run;
    }

    const frame = await this.#request(request);
    this.#expect(frame, "workflow");
    return {
      steps: (frame.steps as Frame[]).map((outcome) => ({
        step: Number(outcome.step),
        nodeId: String(outcome.node_id),
        success: Boolean(outcome.success),
        output: new Uint8Array(
          Buffer.from(String(outcome.output ?? ""), "base64"),
        ),
        durationMs: Number(outcome.duration_ms),
        error: outcome.error as string | undefined,
      })),
      skipped: ((frame.skipped as number[]) ?? []).map(Number),
      resumed: ((frame.resumed as number[]) ?? []).map(Number),
      success: Boolean(frame.success),
    };
  }

  /**
   * What the mesh has moved, saved, run, and queued.
   *
   * Returned as the controller sent it rather than as a typed shape: this is
   * a dashboard feed that grows fields, and a client that gets one it does not
   * know about should see it rather than lose it.
   */
  async stats(): Promise<Record<string, unknown>> {
    const frame = await this.#request({ type: "stats" });
    this.#expect(frame, "stats");
    const { type: _type, ...rest } = frame;
    return rest;
  }

  /**
   * The last few tasks that finished anywhere in the mesh.
   *
   * Not only the ones this connection submitted — a task somebody else ran is
   * exactly the interesting case. The preview is the front of the output, not
   * the output: results stay on the node that produced them.
   */
  async recent(limit = 20): Promise<FinishedTask[]> {
    const frame = await this.#request({ type: "recent", limit });
    this.#expect(frame, "recent");
    return (frame.tasks as Frame[]).map((task) => ({
      taskId: String(task.task_id),
      kind: String(task.kind),
      nodeId: String(task.node_id),
      success: Boolean(task.success),
      durationMs: Number(task.duration_ms),
      outputBytes: Number(task.output_bytes),
      preview: String(task.preview),
      secondsAgo: Number(task.seconds_ago),
    }));
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
      address: String(node.address ?? ""),
      latencyMs: node.latency_ms as number | undefined,
      bandwidthBytesPerSec: node.bandwidth_bytes_per_sec as number | undefined,
      datasetsHeld: Number(node.datasets_held ?? 0),
      bytesHeld: Number(node.bytes_held ?? 0),
      connected: Boolean(node.connected ?? true),
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
    priority?: Priority;
    timeoutMs?: number;
  }): Promise<TaskResult> {
    const frame = await this.#request({
      type: "submit",
      kind: task.kind,
      payload: Buffer.from(task.payload).toString("base64"),
      inputs: task.inputs,
      constraints: task.constraints,
      module: task.module ?? null,
      priority: task.priority ?? null,
      timeout_ms: task.timeoutMs ?? null,
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
