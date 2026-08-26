"""Server fan-out under concurrent sessions: the scaling pillar's baseline numbers.

Each benchmark round publishes a fixed burst — ``UPDATES`` increments across every stream at
``INTERVAL`` — and completes when **every** client has received the final revision of every
stream, so the primary statistic is end-to-end delivery time for the whole fleet. Derived
metrics ride ``extra_info``: delivery-latency percentiles, delivered/published (coalescing),
and the server's CPU, RSS, thread count, and event-loop lag.

Scale the grid with ``TRANSPORTS_BENCH`` (e.g. ``TRANSPORTS_BENCH=1000:10,2000:20``) and the
burst with ``TRANSPORTS_BENCH_UPDATES`` / ``TRANSPORTS_BENCH_INTERVAL``.
"""

from __future__ import annotations

import os
import time

import pytest

UPDATES = int(os.environ.get("TRANSPORTS_BENCH_UPDATES", "50"))
INTERVAL = float(os.environ.get("TRANSPORTS_BENCH_INTERVAL", "0.002"))


def percentile(samples: list[float], q: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


def _grid() -> list[tuple[int, int]]:
    spec = os.environ.get("TRANSPORTS_BENCH")
    if spec:
        return [tuple(int(part) for part in combo.split(":")) for combo in spec.split(",")]
    return [(10, 1), (100, 10), (250, 20)]


@pytest.mark.parametrize(("sessions", "streams"), _grid(), ids=lambda v: str(v))
def test_fanout(benchmark, fanout_server, client_fleet, sessions: int, streams: int) -> None:
    server = fanout_server.start(models=streams)
    client_fleet.start(sessions, server.ws_url)
    server.stats()  # drop startup-window lag samples

    cpu_before = server.process.cpu_times()
    wall_before = time.time()

    def run_round() -> None:
        target = server.bump(UPDATES, INTERVAL)
        client_fleet.wait_round(target, streams)

    benchmark.pedantic(run_round, rounds=3, iterations=1, warmup_rounds=1)

    cpu_after = server.process.cpu_times()
    wall = time.time() - wall_before
    cpu_seconds = (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)
    published = 4 * UPDATES * streams  # warmup + 3 measured rounds
    stats = server.stats()

    benchmark.extra_info.update(
        {
            "sessions": sessions,
            "streams": streams,
            "updates_per_round": UPDATES,
            "publish_interval_s": INTERVAL,
            "latency_p50_ms": round(percentile(client_fleet.latencies_ms, 0.50), 3),
            "latency_p95_ms": round(percentile(client_fleet.latencies_ms, 0.95), 3),
            "latency_p99_ms": round(percentile(client_fleet.latencies_ms, 0.99), 3),
            "latency_max_ms": round(max(client_fleet.latencies_ms, default=0.0), 3),
            # patches that reached clients / patches published per client: < 1.0 means the
            # 10ms autosync window coalesced bursts (state streams keep only the newest rev)
            "delivered_ratio": round(client_fleet.delivered / (published * sessions), 4) if sessions else 0.0,
            "server_cpu_pct": round(100 * cpu_seconds / wall, 1) if wall else 0.0,
            "server_rss_mb": round(server.process.memory_info().rss / 1e6, 1),
            "server_threads": server.process.num_threads(),
            "server_loop_lag_p99_ms": round(stats["loop_lag_p99_ms"], 3),
            "server_loop_lag_max_ms": round(stats["loop_lag_max_ms"], 3),
        }
    )
