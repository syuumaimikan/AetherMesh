"""A ``concurrent.futures.Executor`` backed by the mesh.

Python code that already fans work out through a thread pool is written against
one interface: ``submit``, ``map``, ``as_completed``, ``shutdown``. This is that
interface, with the machines on the other side of it replaced.

    from aethermesh import MeshExecutor

    with MeshExecutor.connect(port=7100, max_workers=8) as pool:
        upper = pool.module("uppercase.wasm")
        for output in pool.map(upper, [b"one", b"two", b"three"]):
            print(output.decode())

Everything in :mod:`concurrent.futures` works on the futures this returns —
``as_completed``, ``wait``, ``Future.result``, timeouts, cancellation of work
that has not started yet.

**What it cannot do:** run a Python callable. The mesh sends task names and
WebAssembly modules to nodes, never pickled code, which is most of the reason a
node is safe to volunteer. Handing :meth:`MeshExecutor.submit` an ordinary
function raises :class:`TypeError` rather than silently running it locally — a
pool that quietly stops being distributed is worse than one that refuses.
"""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Executor, Future
from dataclasses import dataclass, field, replace
from typing import Callable, Iterable

from . import AetherMesh, AetherMeshError, TaskResult

__all__ = ["MeshExecutor", "MeshTask"]

#: Sentinel that tells a worker thread to close its connection and stop.
_STOP = object()


@dataclass(frozen=True)
class MeshTask:
    """A unit of work the mesh knows how to run, ready to be submitted.

    Build one with :meth:`MeshExecutor.builtin` or :meth:`MeshExecutor.module`.
    It is callable only so that it fits the :class:`~concurrent.futures.Executor`
    signature; calling it directly is a mistake worth reporting.
    """

    kind: str
    module_id: str | None = None
    inputs: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()

    def with_inputs(self, *data_ids: str) -> "MeshTask":
        """The same task, reading these published datasets."""
        return replace(self, inputs=self.inputs + tuple(data_ids))

    def where(self, *constraints: str) -> "MeshTask":
        """The same task, restricted to nodes satisfying these constraints.

        ``"gpu=true"``, ``"region!=us-east"``, or a bare ``"nvme"`` for a label
        that only has to be present.
        """
        return replace(self, constraints=self.constraints + tuple(constraints))

    def __call__(self, payload: bytes = b"") -> bytes:
        raise RuntimeError(
            f"{self!r} runs on the mesh, not in this process — "
            "submit it to a MeshExecutor instead of calling it"
        )


@dataclass
class _Job:
    task: MeshTask
    payload: bytes
    future: "Future[bytes]"
    raw: "Future[TaskResult]" | None = field(default=None)


class MeshExecutor(Executor):
    """Runs mesh tasks through an :class:`~concurrent.futures.Executor` interface.

    Each worker owns its own connection, because one connection matches replies
    to requests by order and cannot be shared. ``max_workers`` is therefore also
    the number of connections held open, and the number of tasks in flight.
    """

    def __init__(self, connect: Callable[[], AetherMesh], max_workers: int = 4) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")

        self._connect = connect
        self._queue: "queue.SimpleQueue[object]" = queue.SimpleQueue()
        self._shutdown = False
        self._lock = threading.Lock()
        self._workers = [
            threading.Thread(target=self._work, name=f"aethermesh-{index}", daemon=True)
            for index in range(max_workers)
        ]
        for worker in self._workers:
            worker.start()

    @classmethod
    def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 7100,
        token: str | None = None,
        tls_ca_path: str | None = None,
        tls_server_name: str | None = None,
        max_workers: int = 4,
    ) -> "MeshExecutor":
        """Opens ``max_workers`` connections to one controller."""

        def factory() -> AetherMesh:
            return AetherMesh.connect(
                host=host,
                port=port,
                token=token,
                tls_ca_path=tls_ca_path,
                tls_server_name=tls_server_name,
            )

        return cls(factory, max_workers=max_workers)

    def builtin(self, kind: str) -> MeshTask:
        """A built-in task: ``echo``, ``hash``, or ``cpu``."""
        return MeshTask(kind=kind)

    def module(self, path: str) -> MeshTask:
        """Publishes a ``.wasm`` file and returns a task that runs it.

        The module is content-addressed, so publishing the same file again from
        another process costs nothing and reaches each node once.
        """
        with self._borrow() as mesh:
            published = mesh.publish_file(path)
        return MeshTask(kind="wasm", module_id=published.data_id)

    def publish(self, data: bytes) -> str:
        """Publishes a dataset and returns its id, for :meth:`MeshTask.with_inputs`."""
        with self._borrow() as mesh:
            return mesh.publish(data).data_id

    def submit(self, fn: MeshTask, /, payload: bytes = b"", **kwargs) -> "Future[bytes]":
        """Queues one task. The future yields the task's output bytes.

        A task that ran and failed raises :class:`AetherMeshError` from
        ``result()``, carrying the message the node reported.
        """
        if not isinstance(fn, MeshTask):
            raise TypeError(
                "MeshExecutor runs mesh tasks, not Python callables. Use "
                "pool.builtin('hash') or pool.module('task.wasm') to get one — "
                "the mesh never ships executable Python to a node."
            )
        if kwargs:
            raise TypeError(f"unexpected keyword arguments: {', '.join(sorted(kwargs))}")

        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot submit after shutdown")
            future: "Future[bytes]" = Future()
            self._queue.put(_Job(task=fn, payload=payload, future=future))
        return future

    def map(self, fn: MeshTask, *iterables: Iterable[bytes], timeout=None, chunksize=1):
        """Runs ``fn`` over every payload, yielding outputs in input order."""
        return super().map(fn, *iterables, timeout=timeout, chunksize=chunksize)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Stops accepting work and closes every connection."""
        with self._lock:
            already = self._shutdown
            self._shutdown = True

        if cancel_futures:
            self._drain()
        if not already:
            for _ in self._workers:
                self._queue.put(_STOP)
        if wait:
            for worker in self._workers:
                worker.join()

    def _drain(self) -> None:
        """Cancels everything still queued. Running tasks are left alone."""
        while True:
            try:
                job = self._queue.get_nowait()
            except queue.Empty:
                return
            if job is _STOP:
                self._queue.put(_STOP)
                return
            if isinstance(job, _Job):
                job.future.cancel()

    def _work(self) -> None:
        """One worker: own a connection, run queued tasks until told to stop."""
        mesh: AetherMesh | None = None
        try:
            while True:
                job = self._queue.get()
                if job is _STOP:
                    return
                assert isinstance(job, _Job)

                if not job.future.set_running_or_notify_cancel():
                    continue

                try:
                    if mesh is None:
                        mesh = self._connect()
                    job.future.set_result(_run(mesh, job))
                except BaseException as error:  # noqa: BLE001 - reported to the caller
                    # A broken connection should not poison every later job on
                    # this worker, so it is dropped and reopened next time.
                    if isinstance(error, (OSError, AetherMeshError)):
                        _close(mesh)
                        mesh = None
                    job.future.set_exception(error)
        finally:
            _close(mesh)

    def _borrow(self):
        """A short-lived connection for publishing, which workers are busy with."""
        return self._connect()


def _run(mesh: AetherMesh, job: _Job) -> bytes:
    task = job.task
    if task.module_id is not None:
        result = mesh.run_wasm(
            task.module_id,
            job.payload,
            inputs=list(task.inputs),
            constraints=list(task.constraints),
        )
    else:
        result = mesh.run(
            task.kind,
            job.payload,
            inputs=list(task.inputs),
            constraints=list(task.constraints),
        )

    if not result.success:
        raise AetherMeshError(result.error or "the task failed on the node")
    return result.output


def _close(mesh: AetherMesh | None) -> None:
    if mesh is not None:
        mesh.close()
