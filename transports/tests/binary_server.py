"""Live server fixture for browser binary and field-ownership conformance."""

import asyncio
from contextlib import asynccontextmanager, suppress

import uvicorn
from pydantic import BaseModel, Field
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute

from transports import Server, Session, autosync, sse_endpoint, ws_endpoint


class Model(BaseModel):
    playing: bool = Field(False, json_schema_extra={"owner": "client"})
    bpm: int = 120
    data: bytes = bytes(range(256)) * 128


model = Model()
session = Session()
mid = session.host(model)
server = Server(session)
for value in (True, False, True):
    session.command(mid, "playing", value)


@asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(autosync(server))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def state(_request):
    return JSONResponse({"id": mid, "playing": model.playing, "bpm": model.bpm, "size": len(model.data), "first": model.data[0]})


async def command(request):
    for value in await request.json():
        session.command(mid, "playing", value)
    return JSONResponse({"ok": True})


app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/state", state),
        Route("/command", command, methods=["POST"]),
        WebSocketRoute("/ws", ws_endpoint(server)),
        Route("/sse", sse_endpoint(server)),
    ],
)

if __name__ == "__main__":
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    sock = config.bind_socket()
    print(f"http://127.0.0.1:{sock.getsockname()[1]}", flush=True)
    uvicorn.Server(config).run(sockets=[sock])
