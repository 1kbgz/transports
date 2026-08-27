# Server fan-out benchmark results

The recorded baseline keeps WebSocket delivery below 7 ms at p95 with 100 concurrent clients,
each receiving 10 state streams. At 250 clients and 20 streams, the server approaches one fully
occupied CPU core: p95 latency rises to 97–99 ms and full-fleet completion takes 1.06 seconds.

## Latency and completion time

| WebSocket clients | Streams per client | Active subscriptions | Fleet completion median | Delivery p50 | Delivery p95 | Delivery p99 | Delivery max |
| ----------------: | -----------------: | -------------------: | ----------------------: | -----------: | -----------: | -----------: | -----------: |
|                10 |                  1 |                   10 |              113–123 ms |   1.4–1.5 ms |       2.5 ms |   3.4–8.0 ms |   3.4–8.1 ms |
|               100 |                 10 |                1,000 |          175.7–176.3 ms |       4.3 ms |   6.6–6.8 ms |  8.4–10.5 ms | 11.6–12.9 ms |
|               250 |                 20 |                5,000 |           1.059–1.061 s | 59.7–60.8 ms | 97.0–99.1 ms |   107–111 ms |   116–123 ms |

Ranges span two clean runs. Fleet completion measures the full round: publish 50 updates to every
stream at 2 ms intervals, then wait until every client has received the final revision of every
stream. Delivery latency measures individual patches from the server timestamp to receipt by a
client.

## Server resource use

| WebSocket clients |        CPU |          RSS | Event-loop lag p99 | Revisions delivered |
| ----------------: | ---------: | -----------: | -----------------: | ------------------: |
|                10 |   2.6–2.7% | 45.7–52.9 MB |         0.3–0.7 ms |          22.0–22.5% |
|               100 | 44.3–45.0% | 54.8–54.9 MB |         4.7–4.8 ms |          32.0–33.0% |
|               250 | 92.1–92.3% | 75.6–75.7 MB |       17.5–18.0 ms |          96.0–97.0% |

CPU is process CPU use, where 100% represents one fully occupied logical core. Revisions delivered
below 100% reflect expected coalescing, not data loss: transports synchronizes state, so its 10 ms
autosync window can replace intermediate revisions while every client still receives the final one.

## Recorded workload

These ranges come from two clean runs recorded on August 27, 2026:

- transports 0.8.2 at commit `60cb98f`
- one server process and all clients on the same machine over loopback
- Apple silicon (`arm64`, 18 logical CPUs, 64 GiB RAM), macOS 25.5
- CPython 3.12.13, JSON codec, unbatched frames
- 50 updates per stream per round, published every 2 ms
- one warm-up round followed by three measured rounds

This is a single-machine fan-out baseline, not a capacity limit. It excludes network latency and does
not predict cross-region or multi-host performance. Fleet completion also includes the approximately
100 ms update-publishing interval, so it is broader than patch delivery latency.

## Session budgets

The session-cost benchmark (`benchmarks/test_session_cost.py`) measures what a connected-but-idle
session costs the server, and both benchmarks derive the budget metrics below. Measured on the
recording machine at 1,000 clients × 10 streams:

| Budget                                  |   Target | Measured |
| --------------------------------------- | -------: | -------: |
| RSS per idle session                    | < 100 KB |  73.7 KB |
| Idle server CPU, whole fleet connected  |     < 5% |     0.4% |
| CPU per state fan-out to 1k subscribers |   < 1 ms |  0.92 ms |
| Thread growth across 1,000 connects     |        0 |        0 |

Thread flatness is asserted unconditionally on every run — a session must never cost a thread. The
absolute budgets are opt-in (`TRANSPORTS_BUDGETS=1`) because hosted CI runners are too noisy for
stable thresholds; run them locally or on the recording machine. The fan-out CPU budget applies
only at 250+ sessions (`TRANSPORTS_BUDGET_FANOUT_MIN_SESSIONS`) where fixed per-publish cost
amortizes, and currently passes with under 10% headroom — a regression that trips it is real.

## Event loop comparison

The harness can swap the server's event loop (`TRANSPORTS_BENCH_LOOP=asyncio|uvloop|rsloop`; the
client fleet always stays on asyncio so only the measured process varies). At 1,000 clients × 10
streams on the recording machine, ranges over two runs each:

| Loop    | Delivery p50 | Delivery p99 | CPU per 1k-subscriber fan-out | Server CPU | RSS per idle session |
| ------- | -----------: | -----------: | ----------------------------: | ---------: | -------------------: |
| asyncio |     82–89 ms |   173–174 ms |                  0.90–0.92 ms |     68–69% |              73.6 KB |
| uvloop  |   124–127 ms |   245–250 ms |                  0.97–1.02 ms |     68–70% |              73.2 KB |
| rsloop  |     32–32 ms |   103–113 ms |                  1.21–1.24 ms |        88% |               124 KB |

No loop dominates. rsloop delivers dramatically lower latency — it appears to flush writes far
more eagerly — but spends ~34% more CPU per fan-out and ~70% more memory per idle session,
breaking both budgets above. uvloop batches more aggressively than asyncio (lowest event-loop
lag) but that shows up as *higher* delivery latency under this fan-out shape. asyncio remains the
default: it wins both budget metrics, and the latency headroom (p99 well under the 150 ms target)
does not yet justify rsloop's CPU cost on a small-instance budget. Worth revisiting once encode/
diff hot paths move into Rust and CPU headroom opens up — and re-measuring on Linux, where these
loops' relative standings are known to differ from macOS.

## Interactive results

The report below is generated from committed benchmark records. It exposes the recorded runs,
benchmark parameters, timing distributions, and machine metadata.

```{benched} ../../benchmarks/results
:view: trend
:metric: median
:benchmark-filter: test_fanout[[]*[]]
:x-axis: time
```

The [full interactive report](https://1kbgz.github.io/transports/benchmarks/) includes all committed
timing runs and filters.
