"""Live demo — a Python ``Session`` hosts a model over WebSocket; the browser mirrors it.

    pip install "transports[connections]" uvicorn
    (cd js && pnpm build)          # builds the wasm the page loads (js/dist/pkg)
    python examples/server.py      # then open http://127.0.0.1:8000

The Python side increments a counter; the browser updates live, applying patches with the wasm core.
There is no per-frame Python round-trip and no hand-written sync code on either side — just the model
and the incremental patches the `Session` emits.
"""

import asyncio
from pathlib import Path

from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.responses import FileResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles

import transports

HERE = Path(__file__).parent
PKG = HERE.parent / "js" / "dist" / "pkg"  # wasm-bindgen output the browser loads


class Counter(BaseModel):
    label: str = "ticks"
    tick: int = 0


session = transports.Session()
counter = Counter()
session.host(counter)
server = transports.Server(session)


async def _index(request):
    return FileResponse(HERE / "index.html")


async def _ticker():
    while True:
        await asyncio.sleep(0.5)
        counter.tick += 1  # mutate the model; the Session emits a patch on the next flush


async def _startup():
    asyncio.create_task(transports.autoflush(server, 0.05))  # broadcast patches to all clients
    asyncio.create_task(_ticker())


app = Starlette(
    routes=[
        Route("/", _index),
        WebSocketRoute("/ws", transports.starlette_endpoint(server)),
        Mount("/pkg", app=StaticFiles(directory=str(PKG))),
    ],
    on_startup=[_startup],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
