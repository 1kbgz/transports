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


@pytest.mark.parametrize("codec", ["json", "msgpack", "cbor"])
def test_comm_and_anywidget_adapters_accept_builtin_codecs(codec):
    server = transports.Server(transports.Session())
    transports.serve_anywidget(server, _FakeWidget(), codec=codec)
    transports.serve_comm(server, _FakeComm(), codec=codec)


def test_comm_and_anywidget_adapters_reject_unknown_codecs():
    server = transports.Server(transports.Session())
    with pytest.raises(ValueError):
        transports.serve_anywidget(server, _FakeWidget(), codec="unknown")
    with pytest.raises(ValueError):
        transports.serve_comm(server, _FakeComm(), codec="unknown")
