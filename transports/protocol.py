"""The connection wire protocol: one logical frame per message, as JSON.

WebSocket messages (and Jupyter comm messages) are self-delimiting, so the binary `Frame` envelope
in the Rust core — which exists for byte-stream transports like TCP — isn't needed here. A small
JSON envelope carries the routing metadata around a model snapshot or a patch.

Two message kinds:

- ``{"t": "snapshot", "id": <int>, "type": <str>, "rev": <int>, "value": <Value>}``
- ``{"t": "patch", "id": <int>, "patch": {"rev": <int>, "ops": [...]}}``
"""

import json
from typing import Any


def snapshot_msg(model_id: int, type_name: str, rev: int, value: Any) -> str:
    return json.dumps({"t": "snapshot", "id": model_id, "type": type_name, "rev": rev, "value": value})


def patch_msg(model_id: int, patch: dict) -> str:
    return json.dumps({"t": "patch", "id": model_id, "patch": patch})


def parse(text: str) -> dict:
    return json.loads(text)
