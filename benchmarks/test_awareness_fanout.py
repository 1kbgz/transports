"""Scoped awareness encoding/fan-out cost and retained state, excluding socket delivery."""

import threading

import pytest
from pydantic import BaseModel

from transports import Hub, protocol


class Board(BaseModel):
    title: str = "Board"


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
@pytest.mark.parametrize("peers", [10, 100, 1000])
@pytest.mark.parametrize("scopes", [1, 3])
def test_scoped_awareness_fanout(benchmark, codec, peers, scopes):
    hub = Hub(key=lambda connection: connection)
    sid = hub.share(Board())
    for peer in range(peers):
        hub.subscribe(peer, sid)
        hub.open(peer, codec=codec)
    payload = {"record": "docs/README.md", "field": "title", "pointer": {"x": 0.5, "y": 0.25}}
    frames = [protocol.encode(protocol.awareness_msg(sid, payload, scope=f"surface-{scope}"), codec) for scope in range(scopes)]
    # Seed each scope so repeated updates must replace retained state, not append history.
    for peer in range(peers):
        for frame in frames:
            hub.recv(peer, frame)
    threads = threading.active_count()

    def update():
        total_bytes = 0
        messages = 0
        for frame in frames:
            output = hub.recv(0, frame)
            messages += sum(len(wires) for wires in output.values())
            total_bytes += sum(len(wire.encode() if isinstance(wire, str) else wire) for wires in output.values() for wire in wires)
        return messages, total_bytes

    messages, total_bytes = benchmark.pedantic(update, rounds=5, iterations=1)
    assert messages == (peers - 1) * scopes
    assert sum(len(states) for states in hub._awareness.values()) == peers * scopes
    assert threading.active_count() == threads
    benchmark.extra_info.update(
        peers=peers,
        scopes=scopes,
        codec=codec,
        delivered_frames=messages,
        encoded_bytes=total_bytes,
        retained_states=peers * scopes,
        threads=threads,
    )
    for peer in range(peers):
        hub.close(peer)
    assert not hub._awareness
    assert not hub._peer_ids
