import pytest

import transports


class _FakeWidget:
    def on_msg(self, cb):
        pass

    def send(self, *args, **kwargs):
        pass


class _FakeComm:
    def send(self, *args, **kwargs):
        pass

    def on_msg(self, cb):
        pass

    def on_close(self, cb):
        pass


def test_comm_and_anywidget_adapters_reject_non_json_codecs():
    # these transports carry JSON-able data only; a binary codec would send raw bytes the frontend
    # can't decode from comm/custom-message data, so the adapters reject it up front.
    server = transports.Server(transports.Session())
    with pytest.raises(ValueError):
        transports.serve_anywidget(server, _FakeWidget(), codec="msgpack")
    with pytest.raises(ValueError):
        transports.serve_comm(server, _FakeComm(), codec="msgpack")
