from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .anywidget import serve_anywidget, widget
from .backplane import Backplane, QueueBackplane, UnixSocketBackplane, ZmqBackplane, serve_zmq_broker
from .client import Client
from .comm import serve_comm
from .crdt import CrdtPolicy, CrdtSpec, MapPolicy, RegisterPolicy, SequencePolicy, SetPolicy
from .election import Election
from .hub import READ, WRITE, DeepLwwCrdt, Hub, LastWriteWins, LwwMapCrdt, MergeStrategy
from .protocol import decode_as, encode_as, register_codec, registered_codecs, unregister_codec  # registry-aware wrappers
from .relay import RelayBroadcaster
from .seq import SeqCrdt, seq_delete, seq_insert, seq_key_between, seq_materialize, seq_new
from .server import Server, autosync, sync, ws_endpoint
from .session import Session
from .sse import sse_endpoint
from .transports import (  # compiled Rust extension (rust/python)
    ClientState,
    Store,
    apply,
    cbor_to_json,
    decode,
    decode_message,
    diff,
    encode,
    encode_message,
    json_to_cbor,
    json_to_msgpack,
    msgpack_to_json,
    normalize_message,
)

__version__ = "0.8.6"

__all__ = [
    "READ",
    "WRITE",
    # cross-process backplane (multi-worker fan-out) + clustering
    "Backplane",
    "Client",
    "ClientState",
    "CrdtPolicy",
    "CrdtSpec",
    "DeepLwwCrdt",
    "Election",
    # multi-tenancy + sharing
    "Hub",
    "LastWriteWins",
    "LwwMapCrdt",
    "MapPolicy",
    "MergeStrategy",
    "QueueBackplane",
    "RegisterPolicy",
    "RelayBroadcaster",
    # sequence CRDT (order-free)
    "SeqCrdt",
    "SequencePolicy",
    # connections (WebSocket / SSE / Jupyter comm / anywidget)
    "Server",
    # model bridge + reactive session (high-level)
    "Session",
    "SetPolicy",
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
    "decode_message",
    "diff",
    "encode",
    "encode_as",
    "encode_message",
    "from_value",
    "json_to_cbor",
    "json_to_msgpack",
    "msgpack_to_json",
    "normalize_message",
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
    "widget",
    "ws_endpoint",
]
