# How to benchmark server fan-out

This guide shows you how to run the fan-out benchmark harness, scale it, and record durable
benchmark history with [benched](https://github.com/1kbgz/benched).

The harness in `benchmarks/` is an ordinary pytest-benchmark suite. Each benchmark starts a real
server subprocess, connects a fleet of WebSocket clients, publishes a burst of updates across every
stream, and completes when every client has received the final revision of every stream — the
primary statistic is end-to-end delivery time for the whole fleet. Derived metrics ride each
benchmark's `extra_info`: delivery-latency percentiles, delivered/published ratio (coalescing), and
the server's CPU, RSS, thread count, and event-loop lag.

## Run the harness raw

```bash
make benchmark-py
```

This runs `pytest benchmarks --benchmark-only` and writes plain pytest-benchmark JSON to
`.benchmarks/pytest-benchmark.json`.

## Scale the run

The default grid is `(sessions, streams)` of `(10, 1)`, `(100, 10)`, `(250, 20)`. Environment
variables control the grid and the burst:

| Variable                    | Default              | Meaning                                        |
| --------------------------- | -------------------- | ---------------------------------------------- |
| `TRANSPORTS_BENCH`          | `10:1,100:10,250:20` | Grid as `sessions:streams,...`                 |
| `TRANSPORTS_BENCH_UPDATES`  | `50`                 | Updates published per stream per round         |
| `TRANSPORTS_BENCH_INTERVAL` | `0.002`              | Seconds between publishes                      |
| `TRANSPORTS_BENCH_CODEC`    | `json`               | Wire codec (`json` or `msgpack`)               |
| `TRANSPORTS_BENCH_BATCH`    | unset                | Set to `1` to batch frames per autosync window |

```bash
TRANSPORTS_BENCH=1000:10 TRANSPORTS_BENCH_CODEC=msgpack make benchmark-py
```

## Record and compare runs with benched

benched runs the same suite in an isolated subprocess and records an immutable, commit-aware run
document under `benchmarks/results` — **committed**, so history accumulates in the repo and the
report below always renders from real recorded runs. Record from a consistent machine (runner
hardware is too noisy for durable history; see Continuous integration below). The suite and results
directory are configured in `[tool.benched]` in `pyproject.toml`.

```bash
make benchmark-history           # benched run benchmarks --benchmark-only
```

Inspect and compare recorded history:

```bash
benched list                     # collected benchmarks
benched history                  # recorded runs
benched show latest              # one full run document, extra_info included
benched compare previous latest --metric median
benched report --format html --output build/benchmarks
```

The environment knobs above apply to `benched run` as well; keep the grid consistent when comparing
runs, and compare only runs recorded on the same machine.

## Recorded history

The interactive report below is compiled at docs build time from the committed run history
(default grid: 50 updates per stream per round at 2ms intervals, JSON codec, unbatched; round time
is the median end-to-end delivery time for the full fleet). Delivered ratio below 1.0 is expected:
state streams keep only the newest revision, so the 10ms autosync window coalesces bursts.

```{benched} ../../benchmarks/results
:view: trend
:metric: median
:x-axis: version
```

The [full report](../../benchmarks/index.html) with every view and filter is published alongside these
docs.

## Continuous integration

CI runs the suite at a reduced scale (`TRANSPORTS_BENCH=50:5`, 20 updates per round) on Linux and
uploads the pytest-benchmark JSON as a build artifact for trend inspection. There are no
absolute-latency gates — shared runners are too noisy for stable thresholds — so a CI failure means
the harness itself failed (for example, clients not completing), not that a number moved.
