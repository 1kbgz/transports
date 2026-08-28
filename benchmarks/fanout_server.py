"""The server half of the fan-out benchmark: a Session hosting N stream models served over
`ws_endpoint` + `autosync`, exactly as an application would. Runs as a subprocess so the
server's CPU/RSS/threads are measurable in isolation from the client fleet.

Endpoints:
- ``WS /ws`` — the transports wire.
- ``POST /bump`` ``{"updates": K, "interval": s}`` — start a publish burst: every model's
  ``seq`` increments K times at the given interval, stamping the wall-clock send time.
  Returns ``{"target": final_seq}`` for clients to await.
- ``GET /ready`` — liveness.
- ``GET /stats`` — server-side event-loop lag percentiles since the last read.
"""

from __future__ import annotations

import argparse
import asyncio
import time

import uvicorn
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute

import transports


class Stream(BaseModel):
    seq: int = 0
    stamp: float = 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--models", type=int, default=1)
    parser.add_argument("--loop", choices=("asyncio", "uvloop", "rsloop"), default="asyncio")
    parser.add_argument("--ws-deflate", type=int, choices=(0, 1), default=1)
    parser.add_argument("--flush-interval", type=float, default=0.01)
    args = parser.parse_args()

    if args.loop == "uvloop":
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    elif args.loop == "rsloop":
        import rsloop

        asyncio.set_event_loop_policy(rsloop.EventLoopPolicy())

    session = transports.Session()
    models = [Stream() for _ in range(args.models)]
    for model in models:
        session.host(model)
    server = transports.Server(session)

    lag_samples: list[float] = []

    async def lag_sampler() -> None:
        # measures how late a 5ms sleep fires: the event-loop lag the fan-out induces
        while True:
            started = time.perf_counter()
            await asyncio.sleep(0.005)
            lag_samples.append(time.perf_counter() - started - 0.005)
            if len(lag_samples) > 10_000:
                del lag_samples[: len(lag_samples) - 10_000]

    async def bump(request: Request) -> JSONResponse:
        body = await request.json()
        updates = int(body["updates"])
        interval = float(body.get("interval", 0.0))

        async def publish() -> None:
            for _ in range(updates):
                stamp = time.time()
                for model in models:
                    model.seq += 1
                    model.stamp = stamp
                if interval:
                    await asyncio.sleep(interval)

        asyncio.get_running_loop().create_task(publish())
        return JSONResponse({"target": models[0].seq + updates})

    async def ready(_: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def stats(_: Request) -> JSONResponse:
        samples = sorted(lag_samples)
        lag_samples.clear()
        if not samples:
            return JSONResponse({"loop_lag_p99_ms": 0.0, "loop_lag_max_ms": 0.0})
        return JSONResponse(
            {
                "loop_lag_p99_ms": samples[int(len(samples) * 0.99) - 1] * 1000,
                "loop_lag_max_ms": samples[-1] * 1000,
            }
        )

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", transports.ws_endpoint(server)),
            Route("/bump", bump, methods=["POST"]),
            Route("/ready", ready),
            Route("/stats", stats),
        ],
    )

    async def serve() -> None:
        sync = asyncio.get_running_loop().create_task(transports.autosync(server, interval=args.flush_interval))
        lag = asyncio.get_running_loop().create_task(lag_sampler())
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=args.port,
            log_level="error",
            ws_max_queue=4096,
            ws_per_message_deflate=bool(args.ws_deflate),
        )
        await uvicorn.Server(config).serve()
        sync.cancel()
        lag.cancel()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
