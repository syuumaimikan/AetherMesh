"""A TCP relay that adds latency and a bandwidth ceiling.

Loopback numbers flatter anything that moves bytes: there is no propagation
delay and effectively no ceiling. This sits between the client and the
controller (or between an agent and the controller) and makes the link behave
like a real one — a fixed one-way delay and a token-bucket rate limit — so the
percentile columns mean something.

It is an emulator, not a network: still one machine, still no loss, no jitter,
no competing traffic. Numbers taken through it are labelled "shaped", never
"real".
"""

from __future__ import annotations

import asyncio
import time


class ShapedLink:
    """Listens locally and forwards to `target`, delaying and rate-limiting."""

    def __init__(
        self,
        target_host: str,
        target_port: int,
        latency_ms: float = 25.0,
        bandwidth_bytes_per_sec: int = 12_500_000,
        listen_host: str = "127.0.0.1",
    ) -> None:
        self.target = (target_host, target_port)
        # Applied in each direction, so a round trip costs twice this.
        self.latency = latency_ms / 1000.0
        self.rate = max(bandwidth_bytes_per_sec, 1)
        self.listen_host = listen_host
        self.port = 0
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> int:
        self._server = await asyncio.start_server(
            self._handle, self.listen_host, 0
        )
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(*self.target)
        except OSError:
            writer.close()
            return

        await asyncio.gather(
            self._pump(reader, upstream_writer),
            self._pump(upstream_reader, writer),
            return_exceptions=True,
        )
        for stream in (writer, upstream_writer):
            try:
                stream.close()
            except OSError:
                pass

    async def _pump(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Copies one direction, paying latency once and rate continuously."""
        first = True
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return

            if first:
                # One propagation delay per direction, not per packet: paying it
                # per chunk would model a far worse link than intended.
                await asyncio.sleep(self.latency)
                first = False

            started = time.perf_counter()
            writer.write(chunk)
            await writer.drain()

            # Token bucket, simplified: a chunk of N bytes occupies the link for
            # N / rate seconds, minus however long the write already took.
            occupancy = len(chunk) / self.rate
            remaining = occupancy - (time.perf_counter() - started)
            if remaining > 0:
                await asyncio.sleep(remaining)
