"""Announce hosted model membership to connections before sending their patches."""

import json
from collections.abc import Iterable
from typing import Any

from . import protocol
from .session import Session


class ModelAnnouncements:
    def __init__(self) -> None:
        self._known: dict[Any, tuple[int, set[int]]] = {}

    def open(self, conn: Any, session: Session, extra: Iterable[int] = ()) -> str:
        ids = session.ids()
        self._known[conn] = (session._generation, set(ids))
        return json.dumps({"t": "models", "ids": [*ids, *extra]})

    def flush(self, conn: Any, session: Session, extra: Iterable[int] = ()) -> list[str]:
        generation, known = self._known[conn]
        if generation == session._generation:
            return []
        ids = session.ids()
        messages = [self.open(conn, session, extra)]
        for mid in ids:
            if mid not in known:
                snap = session.snapshot(mid)
                messages.append(protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"]))
        return messages

    def close(self, conn: Any) -> None:
        self._known.pop(conn, None)
