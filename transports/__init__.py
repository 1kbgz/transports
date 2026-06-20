from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .anywidget import flush_anywidget, serve_anywidget
from .client import Client
from .comm import pump_comms, serve_comm
from .hub import READ, WRITE, Hub, LastWriteWins, LwwMapCrdt, MergeStrategy
from .protocol import decode_as, encode_as, register_codec, registered_codecs, unregister_codec  # registry-aware wrappers
from .server import Server, autoflush, starlette_endpoint
from .session import Session
from .sse import sse_endpoint
from .transports import (  # compiled Rust extension (rust/python)
    Store,
    apply,
    decode,
    diff,
    encode,
    json_to_msgpack,
    msgpack_to_json,
)

__version__ = "0.3.0"

__all__ = [
    "__version__",
    # core (low-level)
    "Store",
    "apply",
    "decode",
    "decode_as",
    "diff",
    "encode",
    "encode_as",
    "json_to_msgpack",
    "msgpack_to_json",
    # custom wire codecs
    "register_codec",
    "unregister_codec",
    "registered_codecs",
    # model bridge + reactive session (high-level)
    "Session",
    "to_value",
    "from_value",
    "schema_of",
    "schema_to_ts",
    # connections (WebSocket / SSE / Jupyter comm)
    "Server",
    "Client",
    "starlette_endpoint",
    "autoflush",
    "sse_endpoint",
    "serve_comm",
    "pump_comms",
    "serve_anywidget",
    "flush_anywidget",
    "protocol",
    # multi-tenancy + sharing
    "Hub",
    "READ",
    "WRITE",
    "MergeStrategy",
    "LastWriteWins",
    "LwwMapCrdt",
]
