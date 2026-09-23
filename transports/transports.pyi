"""Type stubs for the compiled Rust extension (built from `rust/python`)."""

def diff(old: str, new: str) -> str:
    """Diff two JSON-encoded models, returning the JSON-encoded patch."""

def apply(value: str, patch: str) -> str:
    """Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result."""

def normalize_crdt_spec(json: str) -> str:
    """Parse, validate, and deterministically serialize a CRDT specification."""

def crdt_spec_hash(json: str) -> str:
    """Return the deterministic SHA-256 hash of a CRDT specification."""

def require_crdt_spec_hash(json: str, peer_hash: str) -> None:
    """Reject a peer hash that does not match the local CRDT specification."""

def encode(value: str) -> bytes:
    """Encode a JSON-encoded model to codec bytes."""

def decode(data: bytes) -> str:
    """Decode codec bytes back to a JSON-encoded model string."""

def encode_as(value: str, codec: str) -> bytes:
    """Encode a JSON-encoded model with the codec named by `codec` (e.g. "application/msgpack")."""

def decode_as(data: bytes, codec: str) -> str:
    """Decode bytes (from `codec`'s codec) back to a JSON-encoded model string."""

def json_to_msgpack(json: str) -> bytes:
    """Convert an arbitrary JSON document to MessagePack bytes."""

def msgpack_to_json(data: bytes) -> str:
    """Convert MessagePack bytes back to a JSON document."""

def normalize_message(json: str) -> str:
    """Parse and serialize one typed live protocol message as compact JSON."""

def encode_message(json: str, codec: str) -> bytes:
    """Encode one JSON live protocol message with a built-in connection codec."""

def decode_message(data: bytes, codec: str) -> str:
    """Decode one built-in connection-codec payload as typed live protocol message JSON."""

class ClientState:
    """Shared revision and proposal reducer for a language-level client adapter."""

    def prepare(self, message_json: str) -> str: ...
    def commit(self, effect_json: str) -> None: ...
    def proposal(self, id: int, ops_json: str, proposal: str | None = None) -> str: ...
    def disconnect(self) -> str: ...
    def revisions(self) -> str: ...
    def pending(self) -> list[str]: ...
    def abandon(self, proposal: str) -> bool: ...

class CrdtDocument:
    """Schema-directed CRDT reducer backed by the shared core."""

    def __init__(self, spec_json: str, value_json: str, replica: str) -> None: ...
    @staticmethod
    def from_state(spec_json: str, state_json: str, replica: str) -> CrdtDocument: ...
    def value(self) -> str: ...
    def state(self) -> str: ...
    def mutate(self, mutations_json: str) -> str: ...
    def apply(self, ops_json: str) -> str: ...
    def member_key(self, path_json: str, value_json: str) -> str: ...
    def compact(self, frontier_json: str) -> int: ...

class Store:
    """In-process model store: host / mutate -> patch / apply / snapshot."""

    def __init__(self) -> None: ...
    def host(self, type_name: str, value_json: str) -> int: ...
    def snapshot(self, id: int) -> str | None: ...
    def mutate(self, id: int, value_json: str) -> str | None: ...
    def apply(self, id: int, patch_json: str) -> bool: ...
