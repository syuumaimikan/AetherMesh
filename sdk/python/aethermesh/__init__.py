"""AetherMesh client for Python.

The wire format is four bytes of big-endian length followed by one JSON object,
in both directions — so this module is a socket, a struct, and json.

    from aethermesh import AetherMesh

    with AetherMesh.connect(port=7100) as mesh:
        data = mesh.publish(open("input.bin", "rb").read())
        result = mesh.run("hash", b"seed", inputs=[data.data_id])
        print(result.output.hex())
"""

from __future__ import annotations

import base64
import json
import socket
import ssl
import struct
from dataclasses import dataclass
from types import TracebackType

__all__ = [
    "AetherMesh",
    "AetherMeshError",
    "FinishedTask",
    "MeshExecutor",
    "MeshTask",
    "NodeSummary",
    "Published",
    "Step",
    "StepOutcome",
    "TaskResult",
    "WorkflowResult",
]

#: Refuse to allocate more than this for one response.
MAX_FRAME_BYTES = 256 * 1024 * 1024


class AetherMeshError(RuntimeError):
    """The controller answered with an error, or the connection failed."""


@dataclass(frozen=True)
class Published:
    """A dataset the controller now holds."""

    data_id: str
    size_bytes: int


@dataclass(frozen=True)
class TaskResult:
    """What a task produced.

    A task that ran and failed has ``success`` false and an ``error``; only
    transport and protocol problems raise.
    """

    task_id: str
    node_id: str
    success: bool
    output: bytes
    duration_ms: float
    error: str | None = None


@dataclass(frozen=True)
class NodeSummary:
    """One node in the mesh."""

    node_id: str
    hostname: str
    cpu_cores: int
    cpu_usage: float
    memory_usage: float
    labels: dict[str, str]
    address: str = ""
    latency_ms: float | None = None
    bandwidth_bytes_per_sec: int | None = None
    #: Datasets this node already holds, and their total size. Work reading
    #: them costs no transfer, which is what the scheduler is deciding on.
    datasets_held: int = 0
    bytes_held: int = 0
    #: Registered is not the same as reachable: a node keeps its registration
    #: until its heartbeat times out, because one late heartbeat is not a death.
    connected: bool = True


@dataclass(frozen=True)
class Step:
    """One step of a workflow.

    ``depends_on`` holds indices into the list of steps. Every dependency's
    output becomes an input of the step waiting for it, so a step reads what
    the steps before it produced — and, because the mesh knows which node holds
    that output, reads it without moving it.
    """

    kind: str
    payload: bytes = b""
    depends_on: tuple[int, ...] = ()
    inputs: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    module: str | None = None


@dataclass(frozen=True)
class StepOutcome:
    """What one step of a workflow did."""

    #: Which step of the submitted workflow this is — not its position in the
    #: reply, which differs as soon as any step is skipped or resumed.
    step: int
    node_id: str
    success: bool
    output: bytes
    duration_ms: float
    error: str | None = None


@dataclass(frozen=True)
class WorkflowResult:
    """What a workflow produced."""

    steps: list[StepOutcome]
    #: Steps never attempted because something they depend on failed.
    skipped: list[int]
    #: Steps an earlier run of the same name had already finished. Only ever
    #: non-empty for a named run against a controller with a checkpoint file.
    resumed: list[int]
    success: bool


@dataclass(frozen=True)
class FinishedTask:
    """One task that finished anywhere in the mesh."""

    task_id: str
    kind: str
    node_id: str
    success: bool
    duration_ms: float
    #: Size of the whole output, of which ``preview`` is the front.
    output_bytes: int
    #: The first bytes of the output, with anything unprintable replaced.
    preview: str
    seconds_ago: float


class AetherMesh:
    """A connection to an AetherMesh controller."""

    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock
        self._buffer = bytearray()

    @classmethod
    def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 7100,
        token: str | None = None,
        tls_ca_path: str | None = None,
        tls_server_name: str | None = None,
        timeout: float = 120.0,
    ) -> "AetherMesh":
        """Opens a connection and completes the handshake.

        Setting ``tls_ca_path`` switches to TLS and verifies the controller
        against that certificate — for a self-signed deployment, its own.
        """
        sock = socket.create_connection((host, port), timeout=timeout)
        if tls_ca_path:
            context = ssl.create_default_context(cafile=tls_ca_path)
            sock = context.wrap_socket(sock, server_hostname=tls_server_name or host)

        mesh = cls(sock)
        welcome = mesh._request({"type": "hello", "token": token})
        if welcome.get("type") != "welcome":
            mesh.close()
            raise AetherMeshError(welcome.get("message", "handshake refused"))
        return mesh

    def publish(self, data: bytes) -> Published:
        """Stores data on the controller. Identical bytes yield the same id."""
        frame = self._request({"type": "publish", "data": base64.b64encode(data).decode()})
        self._expect(frame, "published")
        return Published(data_id=frame["data_id"], size_bytes=int(frame["size_bytes"]))

    def publish_file(self, path: str) -> Published:
        """Publishes a file, e.g. a compiled ``.wasm`` module."""
        with open(path, "rb") as handle:
            return self.publish(handle.read())

    def run(
        self,
        kind: str,
        payload: bytes = b"",
        inputs: list[str] | None = None,
        constraints: list[str] | None = None,
    ) -> TaskResult:
        """Runs a built-in task: ``echo``, ``hash``, or ``cpu``.

        ``inputs`` are ids from :meth:`publish`; the mesh moves them to the
        chosen node only if that node does not already hold them.

        ``constraints`` restrict which nodes may run this at all, written as
        ``"gpu=true"``, ``"region!=us-east"``, or ``"nvme"`` (label present).
        A task nothing satisfies raises rather than running somewhere wrong.
        """
        return self._submit(kind, payload, inputs or [], constraints or [], None)

    def run_wasm(
        self,
        module_id: str,
        payload: bytes = b"",
        inputs: list[str] | None = None,
        constraints: list[str] | None = None,
    ) -> TaskResult:
        """Runs a WebAssembly module previously published."""
        return self._submit("wasm", payload, inputs or [], constraints or [], module_id)

    def workflow(
        self,
        steps: list[Step],
        run: str | None = None,
    ) -> WorkflowResult:
        """Runs several tasks, each after the ones it depends on.

        ``run`` names the run so that submitting the same workflow again
        resumes it rather than repeating it: steps that already finished are
        skipped, provided their output is still on a node. It needs a
        controller started with ``checkpoint_path``; without one the name is
        accepted and the workflow runs from the start.

        Reusing a name for a *different* workflow raises rather than resuming,
        because skipping step 3 on the strength of some other graph's step 3
        is the one failure here that produces a confident wrong answer.
        """
        request: dict = {
            "type": "workflow",
            "steps": [
                {
                    "kind": step.kind,
                    "payload": base64.b64encode(step.payload).decode(),
                    "depends_on": list(step.depends_on),
                    "inputs": list(step.inputs),
                    "constraints": list(step.constraints),
                    "module": step.module,
                }
                for step in steps
            ],
        }
        if run is not None:
            request["run"] = run

        frame = self._request(request)
        self._expect(frame, "workflow")
        return WorkflowResult(
            steps=[
                StepOutcome(
                    step=int(outcome["step"]),
                    node_id=outcome["node_id"],
                    success=bool(outcome["success"]),
                    output=base64.b64decode(outcome["output"]),
                    duration_ms=float(outcome["duration_ms"]),
                    error=outcome.get("error"),
                )
                for outcome in frame["steps"]
            ],
            skipped=[int(step) for step in frame.get("skipped") or []],
            resumed=[int(step) for step in frame.get("resumed") or []],
            success=bool(frame["success"]),
        )

    def stats(self) -> dict:
        """What the mesh has moved, saved, run, and queued.

        Returned as the controller sent it rather than as a dataclass: this is
        a dashboard feed that grows fields, and a client that gets a new one it
        does not know about should see it, not lose it.
        """
        frame = self._request({"type": "stats"})
        self._expect(frame, "stats")
        return {key: value for key, value in frame.items() if key != "type"}

    def recent(self, limit: int = 20) -> list[FinishedTask]:
        """The last few tasks that finished anywhere in the mesh.

        Not only the ones this connection submitted — a task somebody else ran
        is exactly the interesting case. The preview is the front of the
        output, not the output: results stay on the node that produced them.
        """
        frame = self._request({"type": "recent", "limit": limit})
        self._expect(frame, "recent")
        return [
            FinishedTask(
                task_id=task["task_id"],
                kind=task["kind"],
                node_id=task["node_id"],
                success=bool(task["success"]),
                duration_ms=float(task["duration_ms"]),
                output_bytes=int(task["output_bytes"]),
                preview=task["preview"],
                seconds_ago=float(task["seconds_ago"]),
            )
            for task in frame["tasks"]
        ]

    def nodes(self) -> list[NodeSummary]:
        """Lists the nodes currently in the mesh."""
        frame = self._request({"type": "nodes"})
        self._expect(frame, "nodes")
        return [
            NodeSummary(
                node_id=node["node_id"],
                hostname=node["hostname"],
                cpu_cores=int(node["cpu_cores"]),
                cpu_usage=float(node["cpu_usage"]),
                memory_usage=float(node["memory_usage"]),
                labels=dict(node.get("labels") or {}),
                address=node.get("address", ""),
                latency_ms=node.get("latency_ms"),
                bandwidth_bytes_per_sec=node.get("bandwidth_bytes_per_sec"),
                datasets_held=int(node.get("datasets_held", 0)),
                bytes_held=int(node.get("bytes_held", 0)),
                connected=bool(node.get("connected", True)),
            )
            for node in frame["nodes"]
        ]

    def close(self) -> None:
        """Closes the connection."""
        try:
            self._socket.close()
        except OSError:
            pass

    def __enter__(self) -> "AetherMesh":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _submit(
        self,
        kind: str,
        payload: bytes,
        inputs: list[str],
        constraints: list[str],
        module: str | None,
    ) -> TaskResult:
        frame = self._request(
            {
                "type": "submit",
                "kind": kind,
                "payload": base64.b64encode(payload).decode(),
                "inputs": inputs,
                "constraints": constraints,
                "module": module,
            }
        )
        self._expect(frame, "result")
        return TaskResult(
            task_id=frame["task_id"],
            node_id=frame["node_id"],
            success=bool(frame["success"]),
            output=base64.b64decode(frame["output"]),
            duration_ms=float(frame["duration_ms"]),
            error=frame.get("error"),
        )

    def _expect(self, frame: dict, kind: str) -> None:
        if frame.get("type") != kind:
            raise AetherMeshError(frame.get("message", f"expected {kind}, got {frame.get('type')}"))

    def _request(self, request: dict) -> dict:
        payload = json.dumps(request).encode()
        self._socket.sendall(struct.pack(">I", len(payload)) + payload)
        return self._read_frame()

    def _read_frame(self) -> dict:
        header = self._read_exactly(4)
        (length,) = struct.unpack(">I", header)
        if length > MAX_FRAME_BYTES:
            raise AetherMeshError(f"controller announced a {length} byte frame")
        return json.loads(self._read_exactly(length))

    def _read_exactly(self, count: int) -> bytes:
        while len(self._buffer) < count:
            chunk = self._socket.recv(max(count - len(self._buffer), 65536))
            if not chunk:
                raise AetherMeshError("connection closed by the controller")
            self._buffer.extend(chunk)

        taken = bytes(self._buffer[:count])
        del self._buffer[:count]
        return taken

# Imported last: executor.py needs the names above, so this has to run after
# they exist rather than at the top of the file.
from .executor import MeshExecutor, MeshTask  # noqa: E402
