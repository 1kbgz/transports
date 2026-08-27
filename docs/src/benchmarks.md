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
document under `.benched/results` (gitignored). The suite and results directory are configured in
`[tool.benched]` in `pyproject.toml`.

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

## Baseline

Default grid, 50 updates per stream per round at 2ms intervals, JSON codec, unbatched. Recorded on
an Apple M5 Pro (arm64, macOS), CPython 3.12. Round time is the median end-to-end delivery time for
the full fleet; latency percentiles are per-message delivery latencies.

| Sessions | Streams | Round (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Delivered ratio | Server CPU (%) | Server RSS (MB) |
| -------: | ------: | ---------: | -------: | -------: | -------: | --------------: | -------------: | --------------: |
|       10 |       1 |      123.1 |     1.26 |     3.86 |     7.99 |            0.23 |            2.1 |            66.6 |
|      100 |      10 |      168.9 |     4.32 |    12.48 |    15.81 |            0.22 |           30.3 |            74.6 |
|      250 |      20 |      319.4 |    13.62 |    24.06 |    26.87 |            0.22 |           66.1 |            87.2 |

Delivered ratio below 1.0 is expected: state streams keep only the newest revision, so the 10ms
autosync window coalesces bursts.

## Continuous integration

CI runs the suite at a reduced scale (`TRANSPORTS_BENCH=50:5`, 20 updates per round) on Linux and
uploads the pytest-benchmark JSON as a build artifact for trend inspection. There are no
absolute-latency gates — shared runners are too noisy for stable thresholds — so a CI failure means
the harness itself failed (for example, clients not completing), not that a number moved.
