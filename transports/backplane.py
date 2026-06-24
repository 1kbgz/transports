"""Cross-process backplane: a small bytes pub/sub that relays frames between worker processes.

A single uvicorn/gunicorn worker is one event loop on one core, and the connection fan-out (snapshot
encode + per-tick send to every client) is what saturates it. Running many workers spreads that I/O
across cores — but each worker then has its own in-memory model, so a change applied on one worker never
reaches clients on another. A `Backplane` closes that gap: a worker `publish()`es its frames and
`messages()` yields what the others publish, so every worker can mirror the same model and fan it to its
own clients. Concurrency between workers is resolved the usual way — by the `Session`/`Hub` merge
strategy — since the frames are ordinary transports frames.

`publish(data)` delivers to every *other* process on the bus (a backplane skips its own messages).
Three transports, by how the workers are launched:

- :class:`QueueBackplane` — ``multiprocessing`` queues. For a parent that spawns its own workers (or a
  ``gunicorn --preload`` fork): build the bus in the parent and hand each worker its handle. Not
  address-based, so it does **not** fit ``uvicorn --workers`` (those re-import the app per worker).
- :class:`UnixSocketBackplane` — a Unix-domain-socket broker. Workers connect by path, so it works with
  any worker manager (``uvicorn``/``gunicorn --workers``). Brokerless: the first worker to bind the path
  runs the reflector, the rest connect.
- :class:`ZmqBackplane` — a ZeroMQ ``XPUB``/``XSUB`` proxy. Workers connect by address (``tcp://`` or
  ``ipc://``), so it is also native under ``--workers``. Same brokerless election. Needs ``pyzmq``.

All three share the :class:`Backplane` interface, so the relay code above them is identical.
"""

from __future__ import annotations

import asyncio
import os
import struct
import sys
import threading
from typing import AsyncIterator

_ID_LEN = 16  # per-instance sender id, prefixed to every frame so a backplane can skip its own messages
_CLOSE = object()  # sentinel pushed on stop() to unblock messages()


class Backplane:
    """A cross-process bytes pub/sub. Subclasses implement the transport; the framing + self-filtering
    and the `messages()` iterator are shared. The sender id is set at construction (so it survives being
    pickled to a child); the asyncio plumbing is created in :meth:`start`, inside the worker's loop."""

    def __init__(self) -> None:
        self._id = os.urandom(_ID_LEN)
        self._inbox: asyncio.Queue | None = None
        self._closed = False

    async def start(self) -> None:
        """Set up the transport and begin receiving. Call once, inside the running event loop."""
        self._inbox = asyncio.Queue()
        await self._start()

    async def _start(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    async def publish(self, data: bytes) -> None:  # pragma: no cover - overridden
        """Broadcast `data` to every other process on the bus."""
        raise NotImplementedError

    async def messages(self) -> AsyncIterator[bytes]:
        """Yield frames published by other processes (never this one), until :meth:`stop`."""
        assert self._inbox is not None, "start() the backplane first"
        while not self._closed:
            item = await self._inbox.get()
            if item is _CLOSE:
                break
            yield item

    async def stop(self) -> None:
        self._closed = True
        if self._inbox is not None:
            self._inbox.put_nowait(_CLOSE)

    # --- helpers for subclasses ---
    def _frame(self, data: bytes) -> bytes:
        return self._id + data

    def _deliver(self, framed: bytes) -> None:
        """Hand a raw framed message to `messages()`, unless it's malformed or our own echo."""
        if self._inbox is None or len(framed) < _ID_LEN or framed[:_ID_LEN] == self._id:
            return
        self._inbox.put_nowait(framed[_ID_LEN:])


class ZmqBackplane(Backplane):
    """ZeroMQ ``XPUB``/``XSUB`` pub/sub. Native under ``uvicorn``/``gunicorn --workers`` (connect by
    address). Brokerless: the first process to bind `front`/`back` runs the proxy in a daemon thread, the
    rest connect. ``runs_proxy`` says which won. Defaults bind loopback TCP; pass ``ipc://`` paths to keep
    it off the network."""

    def __init__(self, front: str = "tcp://127.0.0.1:5599", back: str = "tcp://127.0.0.1:5600") -> None:
        super().__init__()
        self._front, self._back = front, back
        self._pub = self._sub = None
        self._task: asyncio.Task | None = None
        self.runs_proxy = False

    def _elect_proxy(self) -> bool:
        import zmq

        ctx = zmq.Context.instance()
        xsub, xpub = ctx.socket(zmq.XSUB), ctx.socket(zmq.XPUB)
        try:
            xsub.bind(self._front)
            xpub.bind(self._back)
        except zmq.ZMQError:
            xsub.close()
            xpub.close()
            return False
        threading.Thread(target=lambda: zmq.proxy(xsub, xpub), daemon=True).start()
        return True

    async def _start(self) -> None:
        import zmq
        import zmq.asyncio

        self.runs_proxy = self._elect_proxy()
        ctx = zmq.asyncio.Context.instance()
        self._pub = ctx.socket(zmq.PUB)
        self._pub.connect(self._front)
        self._sub = ctx.socket(zmq.SUB)
        self._sub.connect(self._back)
        self._sub.setsockopt(zmq.SUBSCRIBE, b"")
        await asyncio.sleep(0.15)  # let the SUB finish connecting before any publish (ZMQ slow joiner)
        self._task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            while not self._closed:
                self._deliver(await self._sub.recv())
        except asyncio.CancelledError:
            pass

    async def publish(self, data: bytes) -> None:
        await self._pub.send(self._frame(data))

    async def stop(self) -> None:
        await super().stop()
        if self._task:
            self._task.cancel()
        if self._pub:
            self._pub.close()
        if self._sub:
            self._sub.close()


class UnixSocketBackplane(Backplane):
    """A Unix-domain-socket reflector. Native under ``uvicorn``/``gunicorn --workers`` (connect by path).
    Brokerless: the first worker to bind `path` runs the reflector (re-broadcasts every frame to all
    connections), the rest connect; everyone — the binder included — connects a client. ``runs_broker``
    says which bound. Frames are length-prefixed."""

    def __init__(self, path: str = "/tmp/transports-backplane.sock") -> None:
        super().__init__()
        self._path = path
        self._server: asyncio.AbstractServer | None = None
        self._peers: set[asyncio.StreamWriter] = set()  # reflector side
        self._writer: asyncio.StreamWriter | None = None  # client side
        self._task: asyncio.Task | None = None
        self._lock_fd: int | None = None
        self.runs_broker = False

    async def _reflect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._peers.add(writer)
        try:
            while True:
                hdr = await reader.readexactly(4)
                (n,) = struct.unpack(">I", hdr)
                frame = hdr + await reader.readexactly(n)
                for w in list(self._peers):
                    w.write(frame)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._peers.discard(writer)
            writer.close()

    async def _start(self) -> None:
        if sys.platform == "win32":  # Unix domain sockets + flock are POSIX-only
            raise RuntimeError("UnixSocketBackplane requires POSIX; use ZmqBackplane on Windows")
        self.runs_broker = await self._elect()
        reader, self._writer = await self._connect()  # everyone, the binder included, connects a client
        self._task = asyncio.create_task(self._read(reader))

    async def _elect(self) -> bool:
        """Elect exactly one broker via an flock — atomic and race-free. (Bind can't elect: asyncio's
        start_unix_server auto-unlinks an existing socket, so every binder would 'win'.) The lock holder
        binds the socket, which auto-clears any stale one left by a prior run; everyone else connects."""
        import fcntl

        self._lock_fd = os.open(self._path + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False  # another process holds the broker lock
        self._server = await asyncio.start_unix_server(self._reflect, path=self._path)
        return True

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        for _ in range(150):  # wait for the elected broker's socket to come up
            try:
                return await asyncio.open_unix_connection(path=self._path)
            except OSError:
                await asyncio.sleep(0.02)
        raise OSError(f"could not connect to backplane at {self._path}")

    async def _read(self, reader: asyncio.StreamReader) -> None:
        try:
            while not self._closed:
                hdr = await reader.readexactly(4)
                (n,) = struct.unpack(">I", hdr)
                self._deliver(await reader.readexactly(n))
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass

    async def publish(self, data: bytes) -> None:
        frame = self._frame(data)
        self._writer.write(struct.pack(">I", len(frame)) + frame)
        await self._writer.drain()

    async def stop(self) -> None:
        await super().stop()
        if self._task:
            self._task.cancel()
        if self._writer:
            self._writer.close()
        if self._server:
            self._server.close()
        if self._lock_fd is not None:
            os.close(self._lock_fd)  # releases the broker flock


def _queue_broker(inbox, outs) -> None:
    """Reflector process for :class:`QueueBackplane`: fan every frame to all worker queues (a backplane
    drops its own by sender id)."""
    while True:
        framed = inbox.get()
        if framed is None:
            return
        for q in outs:
            q.put(framed)


class QueueBackplane(Backplane):
    """``multiprocessing`` queues. For a parent that spawns its own workers (or a ``gunicorn --preload``
    fork): build the bus with :meth:`bus` in the parent and hand each worker its handle. Blocking
    ``Queue.get`` is awaited in a thread, so it composes with asyncio. Not address-based — for
    ``uvicorn --workers`` use :class:`ZmqBackplane` or :class:`UnixSocketBackplane`."""

    def __init__(self, inbox, outbox) -> None:
        super().__init__()
        self._in = inbox  # shared: worker -> broker
        self._out = outbox  # per-worker: broker -> this worker
        self._task: asyncio.Task | None = None

    @classmethod
    def bus(cls, n: int):
        """Create a broker process + `n` worker handles. Returns ``(broker_process, [handles])``; pass one
        handle to each worker you spawn, and ``broker.terminate()`` on shutdown."""
        import multiprocessing as mp

        inbox = mp.Queue()
        outs = [mp.Queue() for _ in range(n)]
        proc = mp.Process(target=_queue_broker, args=(inbox, outs), daemon=True)
        proc.start()
        return proc, [cls(inbox, outs[i]) for i in range(n)]

    async def _start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            while not self._closed:
                framed = await self._loop.run_in_executor(None, self._out.get)
                if framed is None:
                    break
                self._deliver(framed)
        except asyncio.CancelledError:
            pass

    async def publish(self, data: bytes) -> None:
        self._in.put(self._frame(data))

    async def stop(self) -> None:
        await super().stop()
        self._out.put(None)  # unblock the executor get
        if self._task:
            self._task.cancel()
