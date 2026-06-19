from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .client import Client
from .hub import READ, WRITE, Hub, LastWriteWins, LwwMapCrdt, MergeStrategy
from .server import Server, autoflush, starlette_endpoint
from .session import Session
from .transports import (  # compiled Rust extension (rust/python)
    Store,
    apply,
    decode,
    decode_as,
    diff,
    encode,
    encode_as,
    json_to_msgpack,
    msgpack_to_json,
)

__version__ = "0.2.0"

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
    # model bridge + reactive session (high-level)
    "Session",
    "to_value",
    "from_value",
    "schema_of",
    "schema_to_ts",
    # connections (WebSocket)
    "Server",
    "Client",
    "starlette_endpoint",
    "autoflush",
    "protocol",
    # multi-tenancy + sharing
    "Hub",
    "READ",
    "WRITE",
    "MergeStrategy",
    "LastWriteWins",
    "LwwMapCrdt",
]
