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
    "NodeSummary",
    "Published",
    "TaskResult",
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
    ) -> TaskResult:
        """Runs a built-in task: ``echo``, ``hash``, or ``cpu``.

        ``inputs`` are ids from :meth:`publish`; the mesh moves them to the
        chosen node only if that node does not already hold them.
        """
        return self._submit(kind, payload, inputs or [], None)

    def run_wasm(
        self,
        module_id: str,
        payload: bytes = b"",
        inputs: list[str] | None = None,
    ) -> TaskResult:
        """Runs a WebAssembly module previously published."""
        return self._submit("wasm", payload, inputs or [], module_id)

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
        module: str | None,
    ) -> TaskResult:
        frame = self._request(
            {
                "type": "submit",
                "kind": kind,
                "payload": base64.b64encode(payload).decode(),
                "inputs": inputs,
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
