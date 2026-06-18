from . import protocol
from ._bridge import from_value, schema_of, schema_to_ts, to_value
from .client import Client
from .server import Server, autoflush, starlette_endpoint
from .session import Session
from .transports import Store, apply, decode, decode_as, diff, encode, encode_as  # compiled Rust extension (rust/python)

__version__ = "0.1.2"

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
]
