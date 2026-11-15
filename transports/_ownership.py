"""Connection routing over the shared field-authority state machine."""

import json
import uuid
from typing import Any, cast

from . import protocol
from .session import Session


class OwnershipServer:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.peers: dict[Any, str] = {}
        self._eligible: dict[Any, bool] = {}
        self._known: dict[Any, tuple[int, set[int]]] = {}

    def open(self, conn: Any, eligible: bool = True) -> list[str]:
        peer = self.peers.setdefault(conn, uuid.uuid4().hex)
        self._eligible[conn] = eligible
        self._known[conn] = (self.session._generation, set(self.session._authorities))
        messages = [(authority.open(peer) if eligible else authority.grant(peer)) for authority in self.session._authorities.values()]
        for authority in self.session._authorities.values():
            if authority.owner() == peer and (message := authority.next_command()) is not None:
                messages.append(message)
        return messages

    def flush(self) -> dict[Any, list[str]]:
        connections = {peer: conn for conn, peer in self.peers.items()}
        messages: dict[Any, list[str]] = {}
        for conn, peer in self.peers.items():
            generation, known = self._known[conn]
            if generation == self.session._generation:
                continue
            for mid, authority in self.session._authorities.items():
                if mid not in known:
                    grant = authority.open(peer) if self._eligible[conn] else authority.grant(peer)
                    messages.setdefault(conn, []).append(grant)
            self._known[conn] = (self.session._generation, set(self.session._authorities))
        for authority in self.session._authorities.values():
            owner = authority.owner()
            if owner in connections and (message := authority.next_command()) is not None:
                messages.setdefault(connections[owner], []).append(message)
        return messages

    def close(self, conn: Any) -> dict[Any, list[str]]:
        self._known.pop(conn, None)
        self._eligible.pop(conn, None)
        peer = self.peers.pop(conn, None)
        if peer is None:
            return {}
        out: dict[Any, list[str]] = {}
        for authority in self.session._authorities.values():
            if authority.close(peer):
                for target, token in self.peers.items():
                    out.setdefault(target, []).append(authority.grant(token))
        for target, messages in self.flush().items():
            out.setdefault(target, []).extend(messages)
        return out

    def recv(self, conn: Any, message: dict) -> dict[Any, list[str]]:
        mid = message["id"]
        authority = self.session._authorities.get(mid)
        if authority is None or conn not in self.peers:
            return {conn: [protocol.reject_msg(mid, 0, "model has no client-owned fields")]}
        peer = self.peers[conn]
        recovering = False
        try:
            if message["t"] == "command_ack":
                authority.acknowledge(peer, message["epoch"], message["command"])
                return self.flush()
            fresh = authority.validate(peer, json.dumps(message))
            out: dict[Any, list[str]] = {}
            rev = self.session._store.revision(mid)
            if fresh:
                recovering = True
                patch = self.session._submit_owned(mid, message["patch"])
                recovering = False
                rev = patch["rev"]
                authority.commit(message["sequence"])
                wire = protocol.patch_msg(mid, patch)
                out = {target: [wire] for target in self.peers if target != conn}
            out[conn] = [json.dumps({"t": "owned_ack", "id": mid, "epoch": message["epoch"], "sequence": message["sequence"], "rev": rev})]
            return out
        except ValueError as error:
            messages = [protocol.reject_msg(mid, cast(int, self.session._store.revision(mid)), str(error))]
            out = {conn: messages}
            if recovering:
                # Revoke first so the client accepts recovery instead of preserving invalid local status.
                authority.close(peer)
                messages.append(authority.grant(peer))
                snap = self.session.snapshot(mid)
                messages.append(protocol.snapshot_msg(mid, snap["type_name"], snap["rev"], snap["value"]))
                authority.open(peer)
                for target, token in self.peers.items():
                    out.setdefault(target, []).append(authority.grant(token))
                for target, pending in self.flush().items():
                    out.setdefault(target, []).extend(pending)
            else:
                messages.append(authority.grant(peer))
            return out
