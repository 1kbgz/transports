from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .anywidget import serve_anywidget
from .client import Client
from .comm import serve_comm
from .hub import READ, WRITE, DeepLwwCrdt, Hub, LastWriteWins, LwwMapCrdt, MergeStrategy
from .protocol import decode_as, encode_as, register_codec, registered_codecs, unregister_codec  # registry-aware wrappers
from .server import Server, autosync, sync, ws_endpoint
from .session import Session
from .sse import sse_endpoint
from .transports import (  # compiled Rust extension (rust/python)
    Store,
    apply,
    cbor_to_json,
    decode,
    diff,
    encode,
    json_to_cbor,
    json_to_msgpack,
    msgpack_to_json,
)

__version__ = "0.4.0"

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
    "json_to_cbor",
    "cbor_to_json",
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
    # connections (WebSocket / SSE / Jupyter comm / anywidget)
    "Server",
    "Client",
    "ws_endpoint",
    "sse_endpoint",
    "serve_comm",
    "serve_anywidget",
    "autosync",
    "sync",
    "protocol",
    # multi-tenancy + sharing
    "Hub",
    "READ",
    "WRITE",
    "MergeStrategy",
    "LastWriteWins",
    "LwwMapCrdt",
    "DeepLwwCrdt",
]
