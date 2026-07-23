from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .anywidget import serve_anywidget
from .backplane import Backplane, QueueBackplane, UnixSocketBackplane, ZmqBackplane, serve_zmq_broker
from .client import Client
from .comm import serve_comm
from .election import Election
from .hub import READ, WRITE, DeepLwwCrdt, Hub, LastWriteWins, LwwMapCrdt, MergeStrategy
from .protocol import decode_as, encode_as, register_codec, registered_codecs, unregister_codec  # registry-aware wrappers
from .relay import RelayBroadcaster
from .seq import SeqCrdt, seq_delete, seq_insert, seq_key_between, seq_materialize, seq_new
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

__version__ = "0.5.0"

__all__ = [
    "READ",
    "WRITE",
    # cross-process backplane (multi-worker fan-out) + clustering
    "Backplane",
    "Client",
    "DeepLwwCrdt",
    "Election",
    # multi-tenancy + sharing
    "Hub",
    "LastWriteWins",
    "LwwMapCrdt",
    "MergeStrategy",
    "QueueBackplane",
    "RelayBroadcaster",
    # sequence CRDT (order-free)
    "SeqCrdt",
    # connections (WebSocket / SSE / Jupyter comm / anywidget)
    "Server",
    # model bridge + reactive session (high-level)
    "Session",
    # core (low-level)
    "Store",
    "UnixSocketBackplane",
    "ZmqBackplane",
    "__version__",
    "apply",
    "autosync",
    "cbor_to_json",
    "decode",
    "decode_as",
    "diff",
    "encode",
    "encode_as",
    "from_value",
    "json_to_cbor",
    "json_to_msgpack",
    "msgpack_to_json",
    "protocol",
    # custom wire codecs
    "register_codec",
    "registered_codecs",
    "schema_of",
    "schema_to_ts",
    "seq_delete",
    "seq_insert",
    "seq_key_between",
    "seq_materialize",
    "seq_new",
    "serve_anywidget",
    "serve_comm",
    "serve_zmq_broker",
    "sse_endpoint",
    "sync",
    "to_value",
    "unregister_codec",
    "ws_endpoint",
]
