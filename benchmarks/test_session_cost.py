"""Session cost audit: what a session costs the server before any work is done.

The benchmark statistic is the connection storm — wall-clock to establish the whole fleet
(handshake + per-model snapshots). The audit numbers ride ``extra_info``: RSS growth per idle
session, server CPU over an idle window, and thread growth (which must be zero — sessions must
never cost a thread; that is asserted unconditionally).

Absolute budgets are opt-in via ``TRANSPORTS_BUDGETS=1`` (hosted CI runners are too noisy for
stable thresholds): RSS per idle session under ``TRANSPORTS_BUDGET_SESSION_KB`` (default 100,
the roadmap budget) and idle CPU under ``TRANSPORTS_BUDGET_IDLE_CPU_PCT`` (default 5).
Scale the grid with ``TRANSPORTS_BENCH``, the idle window with ``TRANSPORTS_BENCH_IDLE``.
"""

from __future__ import annotations

import os
import time

import pytest
from conftest import LOOP
from test_fanout import _grid

IDLE_WINDOW = float(os.environ.get("TRANSPORTS_BENCH_IDLE", "5.0"))
BUDGETS = os.environ.get("TRANSPORTS_BUDGETS", "") in ("1", "true")
BUDGET_SESSION_KB = float(os.environ.get("TRANSPORTS_BUDGET_SESSION_KB", "100"))
BUDGET_IDLE_CPU_PCT = float(os.environ.get("TRANSPORTS_BUDGET_IDLE_CPU_PCT", "5"))


@pytest.mark.parametrize(("sessions", "streams"), _grid(), ids=lambda v: str(v))
def test_idle_session_cost(benchmark, fanout_server, client_fleet, sessions: int, streams: int) -> None:
    server = fanout_server.start(models=streams)
    time.sleep(1.0)  # let the server finish importing/allocating before the baseline
    rss_before = server.process.memory_info().rss
    threads_before = server.process.num_threads()

    def connection_storm() -> None:
        client_fleet.start(sessions, server.ws_url)

    # one round: a second storm would stack a second fleet onto the same server
    benchmark.pedantic(connection_storm, rounds=1, iterations=1)

    cpu_before = server.process.cpu_times()
    time.sleep(IDLE_WINDOW)
    cpu_after = server.process.cpu_times()
    rss_after = server.process.memory_info().rss
    threads_after = server.process.num_threads()

    idle_cpu_pct = 100 * ((cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)) / IDLE_WINDOW
    rss_kb_per_session = (rss_after - rss_before) / sessions / 1024

    benchmark.extra_info.update(
        {
            "sessions": sessions,
            "streams": streams,
            "idle_window_s": IDLE_WINDOW,
            "loop": LOOP,
            "rss_kb_per_idle_session": round(rss_kb_per_session, 1),
            "idle_cpu_pct": round(idle_cpu_pct, 2),
            "server_threads_before": threads_before,
            "server_threads_after": threads_after,
        }
    )

    # sessions must never cost a thread, on any hardware
    assert threads_after == threads_before, f"thread count grew {threads_before} -> {threads_after} for {sessions} sessions"
    if BUDGETS:
        assert rss_kb_per_session < BUDGET_SESSION_KB, f"{rss_kb_per_session:.1f}KB RSS per idle session exceeds {BUDGET_SESSION_KB}KB"
        assert idle_cpu_pct < BUDGET_IDLE_CPU_PCT, f"{idle_cpu_pct:.2f}% idle CPU exceeds {BUDGET_IDLE_CPU_PCT}%"
