"""Fixtures for the fan-out benchmark: a measurable server subprocess and an asyncio client
fleet. The server runs out-of-process so its CPU, RSS, and thread count are its own; the
fleet drives N real WebSocket connections from a dedicated event loop and records
per-message delivery latency against the server's send stamps (same host, same clock).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import psutil
import pytest
import websockets

from transports import protocol

_SERVER = Path(__file__).with_name("fanout_server.py")
_HUB_SERVER = Path(__file__).with_name("hub_server.py")
# the server's event loop implementation (asyncio | uvloop | rsloop); the client fleet stays
# on asyncio so loop comparisons vary only the measured process
LOOP = os.environ.get("TRANSPORTS_BENCH_LOOP", "asyncio")
# permessage-deflate on the server (profiling showed it re-compresses each shared frame per
# connection — the single largest Python-side fan-out cost)
WS_DEFLATE = os.environ.get("TRANSPORTS_BENCH_WS_DEFLATE", "1") in ("1", "true")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class FanoutServer:
    port: int
    process: psutil.Process

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/ws"

    def bump(self, updates: int, interval: float, **extra: object) -> int:
        request = urllib.request.Request(
            f"{self.url}/bump",
            data=json.dumps({"updates": updates, "interval": interval, **extra}).encode(),
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request) as response:
            return int(json.loads(response.read())["target"])

    def stats(self) -> dict:
        with urllib.request.urlopen(f"{self.url}/stats") as response:
            return json.loads(response.read())


@pytest.fixture
def fanout_server() -> Iterator[_ServerFactory]:
    factory = _ServerFactory()
    try:
        yield factory
    finally:
        factory.stop()


class _ServerFactory:
    def __init__(self) -> None:
        self._procs: list[subprocess.Popen] = []

    def start(self, models: int) -> FanoutServer:
        return self._spawn(_SERVER, ["--models", str(models)])

    def start_hub(self, tenants: int, private: int, shared: int) -> FanoutServer:
        return self._spawn(_HUB_SERVER, ["--tenants", str(tenants), "--private", str(private), "--shared", str(shared)])

    def _spawn(self, script: Path, args: list[str]) -> FanoutServer:
        port = _free_port()
        proc = subprocess.Popen(
            [
                sys.executable,
                str(script),
                "--port",
                str(port),
                *args,
                "--loop",
                LOOP,
                "--ws-deflate",
                str(int(WS_DEFLATE)),
                "--flush-interval",
                os.environ.get("TRANSPORTS_BENCH_FLUSH_INTERVAL", "0.01"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._procs.append(proc)
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=1):
                    return FanoutServer(port=port, process=psutil.Process(proc.pid))
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("benchmark server did not become ready")

    def stop(self) -> None:
        for proc in self._procs:
            proc.terminate()
        for proc in self._procs:
            proc.wait(timeout=10)


@dataclass
class _ClientState:
    seq: dict[int, int] = field(default_factory=dict)


class ClientFleet:
    """N WebSocket clients on one dedicated event loop. Each client tracks the latest ``seq``
    per model and records a latency sample for every received patch (recv wall-clock minus the
    server's ``stamp``). Coalescing under load is expected and measured: ``delivered`` counts
    patches that arrived, while completion is judged by every client reaching the target seq.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._clients: list[_ClientState] = []
        self._tasks: list[asyncio.Task] = []
        self._sockets: list = []
        self.latencies_ms: list[float] = []
        self.delivered = 0

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def start(self, count: int, ws_url) -> None:
        """Connect `count` clients; `ws_url` is one URL for all, or a callable of the client index
        (per-tenant URLs for the hub benchmark)."""
        self._run(self._start(count, ws_url))

    async def _start(self, count: int, ws_url) -> None:
        for i in range(count):
            ws = await websockets.connect(ws_url(i) if callable(ws_url) else ws_url, max_queue=4096)
            state = _ClientState()
            self._sockets.append(ws)
            self._clients.append(state)
            self._tasks.append(asyncio.get_running_loop().create_task(self._recv(ws, state)))

    async def _recv(self, ws, state: _ClientState) -> None:
        try:
            async for raw in ws:
                frame = protocol.decode(raw)
                for message in frame["msgs"] if frame.get("t") == "batch" else [frame]:
                    self._apply(message, state)
        except websockets.ConnectionClosed:
            pass

    def _apply(self, message: dict, state: _ClientState) -> None:
        if message.get("t") == "patch":
            now = time.time()
            seq = stamp = None
            for op in message["patch"].get("ops", []):
                entry = op.get("Set")
                if not entry:
                    continue
                key = entry["path"][0].get("Key")
                value = next(iter(entry["value"].values()))
                if key == "seq":
                    seq = value
                elif key == "stamp":
                    stamp = value
            if seq is not None:
                state.seq[message["id"]] = seq
                self.delivered += 1
            if stamp:
                self.latencies_ms.append((now - stamp) * 1000)
        elif message.get("t") == "snapshot":
            value = message.get("value", {}).get("Map", {})
            seq = value.get("seq", {}).get("Int")
            if seq is not None:
                state.seq[message["id"]] = seq

    def wait_round(self, target: int, models: int, timeout: float = 120.0, id_min: int = 0, id_max: int | None = None) -> None:
        """Wait until every client's tracked models (optionally only ids in [id_min, id_max]) hit
        `target` — the id bounds separate a hub's private models from shared ones (SHARED_ID_BASE)."""
        self._run(self._wait(target, models, timeout, id_min, id_max))

    async def _wait(self, target: int, models: int, timeout: float, id_min: int, id_max: int | None) -> None:
        def tracked(c: _ClientState) -> list[int]:
            return [s for mid, s in c.seq.items() if mid >= id_min and (id_max is None or mid < id_max)]

        def caught_up(c: _ClientState) -> bool:
            seqs = tracked(c)
            return len(seqs) == models and all(s >= target for s in seqs)

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if all(caught_up(c) for c in self._clients):
                return
            if asyncio.get_running_loop().time() > deadline:
                behind = sum(1 for c in self._clients if not caught_up(c))
                raise TimeoutError(f"{behind}/{len(self._clients)} clients did not reach seq {target}")
            await asyncio.sleep(0.01)

    def stop(self) -> None:
        async def _close() -> None:
            for task in self._tasks:
                task.cancel()
            for ws in self._sockets:
                await ws.close()

        self._run(_close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)


@pytest.fixture
def client_fleet() -> Iterator[ClientFleet]:
    fleet = ClientFleet()
    try:
        yield fleet
    finally:
        fleet.stop()
