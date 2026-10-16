"""Private-model host cost without socket or client-process overhead."""

import pytest
from pydantic import BaseModel

from transports import Hub


class Record(BaseModel):
    value: int = 0


@pytest.mark.parametrize("tenants", [100, 1000])
def test_private_patch_cost(benchmark, tenants):
    hub = Hub(key=lambda conn: conn)
    models = []
    for tenant in range(tenants):
        model = Record()
        hub.tenant(tenant).host(model)
        hub.open(tenant)
        models.append(model)

    def setup():
        for model in models:
            model.value += 1

    def flush():
        frames = hub.flush()
        assert len(frames) == tenants
        assert all(len(messages) == 1 for messages in frames.values())

    benchmark.pedantic(flush, setup=setup, rounds=10, iterations=1)
    benchmark.extra_info.update(tenants=tenants, private_models_per_tenant=1, codec="json", sockets=False)
