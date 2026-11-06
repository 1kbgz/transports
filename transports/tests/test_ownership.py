import asyncio
import json
from dataclasses import dataclass, field

import msgspec
import pytest
from pydantic import BaseModel, Field

from transports import Client, Hub, Server, Session, protocol, schema_of, serve_anywidget


class Clock(BaseModel):
    playing: bool = Field(False, json_schema_extra={"owner": "client"})
    bpm: int = Field(120, ge=1)


def playing(value):
    return [{"Set": {"path": [{"Key": "playing"}], "value": {"Bool": value}}}]


def open_client(server, conn, codec="json"):
    client = Client(codec=codec)
    for wire in server.open(conn, codec=codec):
        client.recv(wire)
    return client


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
@pytest.mark.parametrize("hub", [False, True])
def test_owner_updates_locally_and_host_mirrors_without_validation(codec, hub):
    server = Hub(key=lambda _: "tenant") if hub else None
    session = server.tenant("tenant") if server is not None else Session()
    model = Clock()
    mid = session.host(model)
    server = server or Server(session)
    owner = open_client(server, "owner", codec)
    observer = open_client(server, "observer", codec)
    notifications = []
    session.on_patch(lambda mid, patch: notifications.append((mid, patch)))
    assert owner.owns(mid, "playing")
    assert not observer.owns(mid, "playing")
    wire = owner.edit_ops(mid, playing(True))
    assert owner.model(mid, Clock).playing is True
    assert owner.pending_proposals() == []
    assert model.playing is False
    replies = server.recv("owner", wire)
    assert [protocol.decode(w, codec)["t"] for w in replies["owner"]] == ["owned_ack"]
    owner.recv(replies["owner"][0])
    observer.recv(replies["observer"][0])
    assert owner._rev[mid] == observer._rev[mid] == 1
    assert model.playing is True
    assert len(notifications) == 1
    with pytest.raises(ValueError, match="client-owned"):
        model.playing = False
    with pytest.raises(ValueError, match="does not own"):
        observer.edit_ops(mid, playing(False))
    proposal = owner.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": -1}}}])
    messages = server.recv("owner", proposal)["owner"]
    assert any(protocol.decode(w, codec)["t"] == "reject" for w in messages)
    assert model.bpm == 120
    model.bpm = 140
    for message in server.flush()["owner"]:
        owner.recv(message)
    assert owner.model(mid, Clock).bpm == 140


def test_commands_queue_and_handover_preserves_order_and_fences_old_owner():
    session = Session()
    mid = session.host(Clock())
    ids = [session.command(mid, "playing", value) for value in [True, False, True]]
    server = Server(session)
    first = open_client(server, "first")
    second = open_client(server, "second")
    commands, other = [], []
    first.on_command(commands.append)
    second.on_command(other.append)
    assert [c["command"] for c in commands] == ids[:1]
    assert not other
    for wire in server.recv("first", first.command_ack(dict(commands[0])))["first"]:
        first.recv(wire)
    assert [c["command"] for c in commands] == ids[:2]
    obsolete = first.edit_ops(mid, playing(True))
    for wire in server.close("first")["second"]:
        second.recv(wire)
    assert second.owns(mid, "playing")
    assert [c["command"] for c in other] == ids[1:2]
    assert server.recv("first", obsolete) == {}
    # Replay from a different live connection cannot reuse the old epoch.
    assert any(protocol.decode(w)["t"] == "reject" for w in server.recv("second", obsolete)["second"])
    for wire in server.recv("second", second.command_ack(other[0]))["second"]:
        second.recv(wire)
    assert [c["command"] for c in other] == ids[1:]


@pytest.mark.asyncio
async def test_status_coalesces_and_disconnect_cancels_timers():
    session = Session()
    mid = session.host(Clock())
    server = Server(session)
    client = open_client(server, "owner")
    sent = []
    await client.attach(sent.append)
    for index in range(1000):
        assert await client.propose_ops(mid, playing(index % 2 == 0))
    assert sent == []
    assert client.model(mid, Clock).playing is False
    await asyncio.sleep(0.3)
    assert len(sent) == 1
    assert protocol.decode(sent[0])["patch"]["ops"] == playing(False)
    assert client.pending_proposals() == []
    await client.propose_ops(mid, playing(True))
    client.detach(client._sender)
    await asyncio.sleep(0.3)
    assert len(sent) == 1
    assert not client.owns(mid, "playing")


def test_owner_metadata_and_explicit_model_ownership():
    @dataclass
    class Status:
        playing: bool = field(default=False, metadata={"owner": "client"})

    class Struct(msgspec.Struct):
        playing: bool = False

    assert schema_of(Status)["fields"][0]["owner"] == "client"
    session = Session()
    model = Struct()
    mid = session.host(model, owner="client")
    model.playing = True
    with pytest.raises(ValueError, match="client-owned"):
        session.update(mid)
    assert model.playing is False
    assert session.command(mid, "playing", True) == 1


def test_anywidget_views_have_distinct_owners_and_handover():
    class Widget:
        def __init__(self):
            self.sent = []

        def on_msg(self, callback):
            self.callback = callback

        def send(self, content, buffers=()):
            self.sent.append(content)

    session = Session()
    mid = session.host(Clock())
    session.command(mid, "playing", True)
    server = Server(session)
    widget = Widget()
    serve_anywidget(server, widget)
    for view in ["first", "second"]:
        widget.callback(widget, {"ready": True, "ownership": True, "view": view})
    commands = [content for content in widget.sent if json.loads(content["wire"])["t"] == "command"]
    assert [c["view"] for c in commands] == ["first"]
    widget.callback(widget, {"closed": True, "view": "first"})
    commands = [content for content in widget.sent if json.loads(content["wire"])["t"] == "command"]
    assert [c["view"] for c in commands] == ["first", "second"]


def test_legacy_and_receive_only_connections_cannot_claim_ownership():
    session = Session()
    mid = session.host(Clock())
    session.command(mid, "playing", True)
    server = Server(session)
    legacy = server.open("legacy", ownership=False)
    assert not any(protocol.decode(w)["t"] == "command" for w in legacy)
    owner = open_client(server, "owner")
    assert owner.owns(mid, "playing")


def test_rejected_server_edit_does_not_revert_local_owned_status():
    session = Session()
    model = Clock()
    mid = session.host(model)
    server = Server(session)
    client = open_client(server, "owner")
    client.edit_ops(mid, playing(True))  # still local, not delivered to Python
    invalid = client.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": -1}}}])
    for wire in server.recv("owner", invalid)["owner"]:
        client.recv(wire)
    assert model.playing is False
    assert client.model(mid, Clock).playing is True
    assert client.model(mid, Clock).bpm == 120


@pytest.mark.asyncio
async def test_failed_owner_send_hands_queued_command_to_next_connection():
    from contextlib import suppress

    from transports import autosync

    class Connection:
        def __init__(self, fail=False):
            self.fail = fail
            self.messages = []

        async def send_text(self, wire):
            if self.fail:
                raise OSError("disconnected")
            self.messages.append(protocol.decode(wire))

    session = Session()
    mid = session.host(Clock())
    server = Server(session)
    first, second = Connection(fail=True), Connection()
    server.open(first)
    server.open(second)
    session.command(mid, "playing", True)
    task = asyncio.create_task(autosync(server, interval=0.001))
    try:
        for _ in range(100):
            if any(message["t"] == "command" for message in second.messages):
                break
            await asyncio.sleep(0.01)
        assert any(message["t"] == "ownership" and message["owned"] for message in second.messages)
        assert any(message["t"] == "command" for message in second.messages)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def test_shared_models_reject_client_ownership():
    with pytest.raises(ValueError, match="Hub.share does not support ownership"):
        Hub(key=lambda _: "tenant").share(Clock())


def test_server_validation_cannot_rewrite_client_owned_fields():
    from pydantic import field_validator

    class NormalizedClock(Clock):
        @field_validator("playing")
        @classmethod
        def normalize_playing(cls, value):
            return False

    session = Session()
    model = NormalizedClock()
    mid = session.host(model)
    server = Server(session)
    client = open_client(server, "owner")
    for wire in server.recv("owner", client.edit_ops(mid, playing(True)))["owner"]:
        client.recv(wire)
    assert model.playing is True
    proposal = client.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": 140}}}])
    for wire in server.recv("owner", proposal)["owner"]:
        client.recv(wire)
    assert client.value(mid)["Map"]["playing"] == {"Bool": True}
    assert model.playing is True
    assert model.bpm == 140
    session.flush()


def test_status_updates_do_not_serialize_unrelated_binary_fields(monkeypatch):
    import transports.session as session_module

    class Recording(Clock):
        audio: bytes = Field(b"x" * (10 * 1024 * 1024), json_schema_extra={"owner": "client"})

    session = Session()
    mid = session.host(Recording())
    server = Server(session)
    client = open_client(server, "owner")

    original = session_module.to_value

    def owned_fields_only(value, *, include=None):
        assert include == {"playing"}
        return original(value, include=include)

    monkeypatch.setattr(session_module, "to_value", owned_fields_only)
    session.flush()
    replies = server.recv("owner", client.edit_ops(mid, playing(True)))
    assert [protocol.decode(wire)["t"] for wire in replies["owner"]] == ["owned_ack"]
    assert session._models[mid].playing is True


def test_changing_connection_tenant_hands_over_previous_ownership():
    keys = {"first": "old", "second": "old"}
    hub = Hub(key=keys.__getitem__)
    session = hub.tenant("old")
    mid = session.host(Clock())
    open_client(hub, "first")
    second = open_client(hub, "second")
    keys["first"] = "new"
    hub.open("first")
    for wire in hub.flush()["second"]:
        second.recv(wire)
    assert second.owns(mid, "playing")


def test_host_observers_receive_client_status_changes():
    from bigbrother import watch

    model = Clock()
    observed = []
    watch(model, lambda *args: observed.append(args), deepstate=True)
    session = Session()
    mid = session.host(model)
    server = Server(session)
    client = open_client(server, "owner")
    server.recv("owner", client.edit_ops(mid, playing(True)))
    assert observed
    assert model.playing is True


@pytest.mark.asyncio
async def test_immediate_edits_include_pending_status_without_replaying_old_values():
    session = Session()
    model = Clock()
    mid = session.host(model)
    server = Server(session)
    client = open_client(server, "owner")
    sent = []
    await client.attach(sent.append)
    await client.propose_ops(mid, playing(True))
    immediate = client.edit_ops(mid, playing(False))
    server.recv("owner", immediate)
    await client.flush_owned()
    assert sent == []
    assert model.playing is False
    client.detach(client._sender)


@pytest.mark.parametrize("flush_callback", [False, True])
@pytest.mark.parametrize("hub", [False, True])
@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_owner_ack_stays_private_and_side_effects_follow_the_status_update(hub, codec, flush_callback):
    server = Hub(key=lambda _: "tenant") if hub else None
    session = server.tenant("tenant") if server is not None else Session()
    model = Clock()
    mid = session.host(model)
    server = server or Server(session)
    owner = open_client(server, "owner", codec)
    observer = open_client(server, "observer", codec)

    def on_patch(_mid, patch):
        if any(op.get("Set", {}).get("path") == [{"Key": "playing"}] for op in patch["ops"]):
            model.bpm = 140
            if flush_callback:
                session.snapshot(mid)

    session.on_patch(on_patch)
    model.bpm = 130
    replies = server.recv("owner", owner.edit_ops(mid, playing(True)))
    observed = [protocol.decode(wire, codec) for wire in replies["observer"]]
    assert [message["t"] for message in observed] == ["patch", "patch", "patch"]
    assert [message["patch"]["rev"] for message in observed] == [1, 2, 3]
    for client, connection in [(owner, "owner"), (observer, "observer")]:
        for wire in replies[connection]:
            client.recv(wire)
        assert client.model(mid, Clock) == model
    assert model.playing and model.bpm == 140


@pytest.mark.asyncio
@pytest.mark.parametrize("hub", [False, True])
async def test_reconnect_refreshes_unsent_owner_status_even_with_resume_revision(hub):
    server = Hub(key=lambda _: "tenant") if hub else None
    session = server.tenant("tenant") if server is not None else Session()
    mid = session.host(Clock())
    server = server or Server(session)
    client = open_client(server, "owner")
    sender = lambda frame: None
    await client.attach(sender)
    await client.propose_ops(mid, playing(True))
    client.detach(sender)
    server.close("owner")
    wires = server.open("owner", since={mid: 0})
    assert protocol.decode(wires[0])["t"] == "snapshot"
    for wire in wires:
        client.recv(wire)
    assert client.model(mid, Clock).playing is False
    assert client.owns(mid, "playing")


@pytest.mark.asyncio
async def test_managed_command_ack_flushes_status_before_acknowledging():
    session = Session()
    mid = session.host(Clock())
    session.command(mid, "playing", True)
    client = open_client(Server(session), "owner")
    commands, sent = [], []
    client.on_command(commands.append)
    await client.attach(sent.append)
    await client.propose_ops(mid, playing(True))
    await client.ack_command(commands[0])
    assert [protocol.decode(frame)["t"] for frame in sent] == ["owned", "command_ack"]
    client.detach(client._sender)


def test_client_authority_reconciliation_excludes_owned_fields():
    session = Session()
    mid = session.host(Clock())
    client = open_client(Server(session), "owner")
    frame = client._reconcile(mid, {"Map": {"playing": {"Bool": True}, "bpm": {"Int": 140}}})
    assert protocol.decode(frame)["patch"]["ops"] == [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": 140}}}]
    assert "ownership=0" in Client(ownership=False)._connect_url("ws://localhost/ws")


def test_commands_accept_model_values_without_constraining_the_result_type():
    session = Session()
    mid = session.host(Clock())
    session.command(mid, "playing", Clock(bpm=140))
    client = open_client(Server(session), "owner")
    commands = []
    client.on_command(commands.append)
    assert commands[0]["value"]["Map"]["bpm"] == {"Int": 140}


def test_owned_types_and_server_validation_are_independent():
    from datetime import datetime, timezone
    from enum import Enum

    from pydantic import field_validator

    from transports import to_value

    calls = []

    class State(str, Enum):
        STOPPED = "stopped"
        PLAYING = "playing"

    class Position(BaseModel):
        seconds: int = Field(0, ge=0)

        @field_validator("seconds")
        @classmethod
        def check(cls, value):
            calls.append(value)
            return value

    class Player(BaseModel):
        position: Position = Field(default_factory=Position, json_schema_extra={"owner": "client"})
        date: datetime = Field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc), json_schema_extra={"owner": "client"})
        state: State = Field(State.STOPPED, json_schema_extra={"owner": "client"})
        samples: tuple[int, ...] = Field((), json_schema_extra={"owner": "client"})
        bpm: int = Field(120, ge=1)

    model = Player()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    calls.clear()
    values = {"position": {"seconds": -2}, "date": "2026-11-05T00:00:00Z", "state": "playing", "samples": [1, 2]}
    ops = [{"Set": {"path": [{"Key": key}], "value": to_value(value)}} for key, value in values.items()]
    replies = server.recv("owner", owner.edit_ops(mid, ops))
    assert [protocol.decode(w)["t"] for w in replies["owner"]] == ["owned_ack"]
    assert isinstance(model.position, Position) and model.position.seconds == -2
    assert isinstance(model.date, datetime)
    assert model.state is State.PLAYING
    assert model.samples == (1, 2)
    assert calls == []
    for bpm, accepted in [(140, True), (-1, False)]:
        proposal = owner.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": bpm}}}])
        replies = server.recv("owner", proposal)
        assert any(protocol.decode(w)["t"] == "reject" for w in replies["owner"]) is not accepted
    assert model.bpm == 140
    assert model.position.seconds == -2 and calls == []
    model.position.seconds = 99
    with pytest.warns(RuntimeWarning, match="client-owned"):
        session.flush()
    assert model.position.seconds == -2


def test_malformed_owned_type_does_not_poison_host():
    model = Clock()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    ops = [{"Set": {"path": [{"Key": "playing"}], "value": {"Str": "wrong"}}}]
    replies = server.recv("owner", owner.edit_ops(mid, ops))
    assert any(protocol.decode(w)["t"] == "reject" for w in replies["owner"])
    assert model.playing is False
    for wire in replies["owner"]:
        owner.recv(wire)
    assert owner.value(mid)["Map"]["playing"] == {"Bool": False}
    assert owner.owns(mid, "playing")
    assert session.value(mid)["Map"]["playing"] == {"Bool": False}
    model.bpm = 150
    session.flush()
    assert model.bpm == 150


def test_owned_idle_flush_does_not_serialize(monkeypatch):
    import transports.session as module

    session = Session()
    session.host(Clock())

    def unexpected(*args, **kwargs):
        raise AssertionError("idle flush must not serialize owned values")

    monkeypatch.setattr(module, "to_value", unexpected)
    assert session.flush() == []


@pytest.mark.asyncio
async def test_anywidget_lost_view_expires_and_redelivers_command():
    class Widget:
        def __init__(self):
            self.sent = []

        def on_msg(self, callback):
            self.callback = callback

        def send(self, content, buffers=()):
            self.sent.append(content)

    session = Session()
    mid = session.host(Clock())
    session.command(mid, "playing", True)
    server = Server(session)
    widget = Widget()
    serve_anywidget(server, widget, view_timeout=0.09)
    widget.callback(widget, {"ready": True, "ownership": True, "heartbeat": True, "view": "lost"})
    await asyncio.sleep(0.06)
    widget.callback(widget, {"ready": True, "ownership": True, "heartbeat": True, "view": "live"})
    await asyncio.sleep(0.06)
    commands = [m for m in widget.sent if "wire" in m and json.loads(m["wire"])["t"] == "command"]
    assert [m["view"] for m in commands] == ["lost", "live"]
    assert {"expired": True, "view": "lost"} in widget.sent
    widget.callback(widget, {"heartbeat": True, "view": "live"})
    await asyncio.sleep(0.06)
    assert {"expired": True, "view": "live"} not in widget.sent
    widget.callback(widget, {"closed": True, "view": "live"})
    assert not server._ownership.peers


def test_owned_model_retains_server_model_configuration():
    from pydantic import ConfigDict

    class StrictClock(Clock):
        model_config = ConfigDict(strict=True, validate_assignment=True)

    session = Session()
    model = StrictClock()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    replies = server.recv("owner", owner.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Str": "140"}}}]))
    assert any(protocol.decode(w)["t"] == "reject" for w in replies["owner"])
    assert model.bpm == 120
    replies = server.recv("owner", owner.edit_ops(mid, [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": 140}}}]))
    assert not any(protocol.decode(w)["t"] == "reject" for w in replies["owner"])
    assert model.bpm == 140


def test_owned_callback_failure_does_not_discard_accepted_broadcast(caplog):
    session = Session()
    model = Clock()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    observer = open_client(server, "observer")

    def fail(mid, patch):
        raise ValueError("application callback failed")

    session.on_patch(fail)
    replies = server.recv("owner", owner.edit_ops(mid, playing(True)))
    for conn, client in [("owner", owner), ("observer", observer)]:
        for wire in replies[conn]:
            client.recv(wire)
        assert client.value(mid)["Map"]["playing"] == {"Bool": True}
    assert owner.owns(mid, "playing")
    assert model.playing is True
    assert "application callback failed" in caplog.text


@pytest.mark.parametrize("kind", ["pydantic", "msgspec"])
def test_owned_aliases_match_wire_and_python_attributes(kind):
    from typing import Annotated

    from pydantic import ConfigDict
    from pydantic.alias_generators import to_camel

    if kind == "pydantic":

        class Model(BaseModel):
            model_config = ConfigDict(alias_generator=to_camel, serialize_by_alias=True, validate_by_name=True)
            is_playing: bool = Field(False, json_schema_extra={"owner": "client"})
            bpm: int = 120
    else:

        class Model(msgspec.Struct, rename="camel"):
            is_playing: Annotated[bool, msgspec.Meta(extra_json_schema={"owner": "client"})] = False
            bpm: int = 120

    model = Model()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    assert owner.owns(mid, "isPlaying")
    schema = session.schema("Model")
    assert schema is not None and schema["fields"][0]["name"] == "isPlaying"
    replies = server.recv("owner", owner.edit_ops(mid, [{"Set": {"path": [{"Key": "isPlaying"}], "value": {"Bool": True}}}]))
    assert [protocol.decode(w)["t"] for w in replies["owner"]] == ["owned_ack"]
    assert model.is_playing is True
    model.bpm = 140
    session.update(mid)
    assert session.value(mid)["Map"]["isPlaying"] == {"Bool": True}
    assert session.value(mid)["Map"]["bpm"] == {"Int": 140}
    session.command(mid, "is_playing", False)
    assert protocol.decode(server.flush()["owner"][-1])["field"] == "isPlaying"


def test_owned_union_preserves_all_fields_and_dataclass_server_constraints():
    from typing import Annotated

    class Basic(BaseModel):
        a: int = 0

    class Detailed(Basic):
        b: int = 0

    class Model(BaseModel):
        item: Basic | Detailed = Field(default_factory=Basic, json_schema_extra={"owner": "client"})

    model = Model()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    ops = [{"Set": {"path": [{"Key": "item"}], "value": {"Map": {"a": {"Int": 1}, "b": {"Int": 2}}}}}]
    replies = server.recv("owner", owner.edit_ops(mid, ops))
    assert [protocol.decode(w)["t"] for w in replies["owner"]] == ["owned_ack"]
    assert isinstance(model.item, Detailed) and model.item.b == 2

    @dataclass
    class Status:
        playing: bool = field(default=False, metadata={"owner": "client"})
        count: Annotated[int, msgspec.Meta(ge=0)] = 0

    session = Session()
    mid = session.host(Status())
    assert session.submit(mid, {"rev": 0, "ops": [{"Set": {"path": [{"Key": "count"}], "value": {"Int": -1}}}]}) is None
    assert session.value(mid)["Map"]["count"] == {"Int": 0}


def test_synchronous_widget_heartbeat_handshake_still_opens():
    class Widget:
        def __init__(self):
            self.sent = []

        def on_msg(self, callback):
            self.callback = callback

        def send(self, content, buffers=()):
            self.sent.append(content)

    session = Session()
    session.host(Clock())
    server = Server(session)
    widget = Widget()
    serve_anywidget(server, widget)
    widget.callback(widget, {"ready": True, "view": "sync", "heartbeat": True, "ownership": True})
    assert any(json.loads(m["wire"])["t"] == "snapshot" for m in widget.sent)
    widget.callback(widget, {"closed": True, "view": "sync"})
    assert not server._ownership.peers


def test_owned_union_can_switch_from_subclass_to_base():
    class Basic(BaseModel):
        a: int = 0

    class Detailed(Basic):
        b: int = 0

    class Model(BaseModel):
        item: Basic | Detailed = Field(default_factory=Detailed, json_schema_extra={"owner": "client"})

    model = Model()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    replies = server.recv("owner", owner.edit_ops(mid, [{"Set": {"path": [{"Key": "item"}], "value": {"Map": {"a": {"Int": 1}}}}}]))
    assert [protocol.decode(w)["t"] for w in replies["owner"]] == ["owned_ack"]
    assert type(model.item) is Basic and model.item.a == 1
    assert session.replay_stats(mid) == {"patches": 0, "blobs": 0, "bytes": 0}
    assert session.since(mid, 0) is None
    assert "item" in model.model_fields_set


def test_server_model_updates_preserve_setattr_and_fields_set():
    assignments = []

    class Model(BaseModel):
        bpm: int = 120

        def __setattr__(self, name, value):
            assignments.append((name, value))
            super().__setattr__(name, value)

    model = Model()
    session = Session()
    mid = session.host(model)
    assignments.clear()
    assert session.submit(mid, {"rev": 0, "ops": [{"Set": {"path": [{"Key": "bpm"}], "value": {"Int": 140}}}]}) is not None
    assert ("bpm", 140) in assignments
    assert model.model_dump(exclude_unset=True) == {"bpm": 140}


def test_cross_field_validators_still_apply_to_server_edits():
    from pydantic import model_validator

    class Bounds(BaseModel):
        low: int = Field(0, json_schema_extra={"owner": "client"})
        high: int = 10

        @model_validator(mode="after")
        def ordered(self):
            if self.low > self.high:
                raise ValueError("low exceeds high")
            return self

    model = Bounds()
    session = Session()
    mid = session.host(model)
    server = Server(session)
    owner = open_client(server, "owner")
    server.recv("owner", owner.edit_ops(mid, [{"Set": {"path": [{"Key": "low"}], "value": {"Int": 20}}}]))
    assert model.low == 20
    assert session.submit(mid, {"rev": 0, "ops": [{"Set": {"path": [{"Key": "high"}], "value": {"Int": 15}}}]}) is None
    assert session.reject_reason is not None and "low exceeds high" in session.reject_reason
    assert session.submit(mid, {"rev": 0, "ops": [{"Set": {"path": [{"Key": "high"}], "value": {"Int": 25}}}]}) is not None
