"""Client durability: the client reconnects across a server restart, and `authority` decides who wins —
the **server** (client adopts the restarted server's state: refetch-on-reconnect) or the **client** (it
pushes its last-known state back, rectifying a server that came back stale/empty)."""

import asyncio
import socket

from transports import Client, DeepLwwCrdt, Hub
from transports.hub import WRITE


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _serve(hub: Hub, sid: int, port: int, stop: asyncio.Event) -> None:
    import websockets

    async def handler(ws):
        hub.subscribe(ws, sid, WRITE)
        for m in hub.open(ws):
            await ws.send(m)
        try:
            async for frame in ws:
                for conn, msgs in hub.recv(ws, frame).items():
                    for msg in msgs:
                        await conn.send(msg)
        except Exception:
            pass
        finally:
            hub.close(ws)

    async with websockets.serve(handler, "127.0.0.1", port):
        await stop.wait()


async def _until(cond, timeout=6.0) -> bool:
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    while loop.time() - t0 < timeout:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def test_server_authoritative_reconnect_adopts_restarted_state():
    async def go():
        port = _free_port()
        hub1 = Hub(key=lambda ws: ws)
        sid = hub1.share({"Map": {"a": {"Int": 1}}}, "Doc", merge=DeepLwwCrdt)
        stop1 = asyncio.Event()
        s1 = asyncio.create_task(_serve(hub1, sid, port, stop1))
        client = Client()
        runner = asyncio.create_task(client.run(f"ws://127.0.0.1:{port}/", authority="server", retry=0.2))

        assert await _until(lambda: sid in client.ids() and client.value(sid) == {"Map": {"a": {"Int": 1}}})
        stop1.set()
        await s1

        # the server comes back with a DIFFERENT state; a server-authoritative client adopts it
        hub2 = Hub(key=lambda ws: ws)
        hub2.share({"Map": {"b": {"Int": 2}}}, "Doc", merge=DeepLwwCrdt)
        stop2 = asyncio.Event()
        s2 = asyncio.create_task(_serve(hub2, sid, port, stop2))
        assert await _until(lambda: client.value(sid) == {"Map": {"b": {"Int": 2}}}), "did not adopt server"
        runner.cancel()
        stop2.set()
        await s2

    asyncio.run(go())


def test_client_authoritative_reconnect_rectifies_a_stale_server():
    async def go():
        port = _free_port()
        hub1 = Hub(key=lambda ws: ws)
        sid = hub1.share({"Map": {"a": {"Int": 1}}}, "Doc", merge=DeepLwwCrdt)
        stop1 = asyncio.Event()
        s1 = asyncio.create_task(_serve(hub1, sid, port, stop1))
        client = Client()
        runner = asyncio.create_task(client.run(f"ws://127.0.0.1:{port}/", authority="client", retry=0.2))

        assert await _until(lambda: sid in client.ids() and client.value(sid) == {"Map": {"a": {"Int": 1}}})
        stop1.set()
        await s1

        # the server comes back EMPTY (lost its state); a client-authoritative client pushes its copy back
        hub2 = Hub(key=lambda ws: ws)
        hub2.share({"Map": {}}, "Doc", merge=DeepLwwCrdt)
        stop2 = asyncio.Event()
        s2 = asyncio.create_task(_serve(hub2, sid, port, stop2))
        assert await _until(lambda: hub2._shared[sid].value == {"Map": {"a": {"Int": 1}}}), "client did not rectify"
        runner.cancel()
        stop2.set()
        await s2

    asyncio.run(go())
