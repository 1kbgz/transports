"""Multi-tenant hub fan-out: the per-tenant/shared two-tier path under concurrent tenants.

The csp-gateway shape: T tenants, one connection each, every tenant holding P private models (its
own ``Session``) and subscribed to S shared models. Each benchmark round publishes ``UPDATES``
bursts to the selected class(es) and completes when every tenant has the final revision of every
model it can see. Private fan-out is 1 subscriber per model x T tenants; shared fan-out is T
subscribers per model — the two tiers the hub routes differently.

Scale with ``TRANSPORTS_BENCH_HUB`` (e.g. ``1000:2:5`` = tenants:private:shared) and select the
published class with ``TRANSPORTS_BENCH_HUB_MODE`` (``mixed`` | ``private`` | ``shared``).
"""

from __future__ import annotations

import os
import time

import pytest
from conftest import LOOP, WS_DEFLATE
from test_fanout import BUDGET_FANOUT_MIN_SESSIONS, BUDGET_FANOUT_MS, BUDGETS, INTERVAL, UPDATES, percentile

from transports.hub import SHARED_ID_BASE

MODE = os.environ.get("TRANSPORTS_BENCH_HUB_MODE", "mixed")


def _grid() -> list[tuple[int, int, int]]:
    spec = os.environ.get("TRANSPORTS_BENCH_HUB")
    if spec:
        return [tuple(int(part) for part in combo.split(":")) for combo in spec.split(",")]
    return [(50, 2, 2), (250, 2, 5)]


@pytest.mark.parametrize(("tenants", "private", "shared"), _grid(), ids=lambda v: str(v))
def test_hub_fanout(benchmark, fanout_server, client_fleet, tenants: int, private: int, shared: int) -> None:
    server = fanout_server.start_hub(tenants=tenants, private=private, shared=shared)
    query = "&batch=1" if os.environ.get("TRANSPORTS_BENCH_BATCH", "") in ("1", "true") else ""
    client_fleet.start(tenants, lambda i: f"{server.ws_url}?tenant=t{i}{query}")
    server.stats()  # drop startup-window lag samples

    cpu_before = server.process.cpu_times()
    rss_baseline = server.process.memory_info().rss
    wall_before = time.time()

    def run_round() -> None:
        target = server.bump(UPDATES, INTERVAL, mode=MODE)
        if MODE in ("mixed", "private"):
            client_fleet.wait_round(target, private, id_max=SHARED_ID_BASE)
        if MODE in ("mixed", "shared"):
            client_fleet.wait_round(target, shared, id_min=SHARED_ID_BASE)

    benchmark.pedantic(run_round, rounds=3, iterations=1, warmup_rounds=1)

    cpu_after = server.process.cpu_times()
    wall = time.time() - wall_before
    cpu_seconds = (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)
    # per-connection deliverable updates per round, by class; 4 = warmup + 3 measured rounds
    per_conn = (private if MODE in ("mixed", "private") else 0) + (shared if MODE in ("mixed", "shared") else 0)
    published = 4 * UPDATES * per_conn
    cpu_ms_per_kfanout = round(1000 * cpu_seconds / (published * tenants / 1000), 4) if tenants and published else 0.0
    stats = server.stats()

    benchmark.extra_info.update(
        {
            "tenants": tenants,
            "private_models": private,
            "shared_models": shared,
            "mode": MODE,
            "updates_per_round": UPDATES,
            "loop": LOOP,
            "ws_deflate": WS_DEFLATE,
            "latency_p50_ms": round(percentile(client_fleet.latencies_ms, 0.50), 3),
            "latency_p95_ms": round(percentile(client_fleet.latencies_ms, 0.95), 3),
            "latency_p99_ms": round(percentile(client_fleet.latencies_ms, 0.99), 3),
            "delivered_ratio": round(client_fleet.delivered / (published * tenants), 4) if tenants and published else 0.0,
            "server_cpu_pct": round(100 * cpu_seconds / wall, 1) if wall else 0.0,
            "server_cpu_ms_per_kfanout": cpu_ms_per_kfanout,
            "server_rss_kb_per_tenant": round((server.process.memory_info().rss - rss_baseline) / tenants / 1024, 1) if tenants else 0.0,
            "server_rss_mb": round(server.process.memory_info().rss / 1e6, 1),
            "server_threads": server.process.num_threads(),
            "server_loop_lag_p99_ms": round(stats["loop_lag_p99_ms"], 3),
            "server_loop_lag_max_ms": round(stats["loop_lag_max_ms"], 3),
        }
    )

    # the per-1k-subscriber budget describes the shared (broadcast) tier; private-tier patches
    # are unique per tenant, so their cost is per-patch, not per-subscriber — recorded, not gated
    if BUDGETS and MODE == "shared" and tenants >= BUDGET_FANOUT_MIN_SESSIONS:
        assert cpu_ms_per_kfanout < BUDGET_FANOUT_MS, f"{cpu_ms_per_kfanout}ms CPU per fan-out to 1k subscribers exceeds {BUDGET_FANOUT_MS}ms"
