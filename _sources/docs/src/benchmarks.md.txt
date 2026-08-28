# Server fan-out benchmark results

The recorded baseline completes a full 50-update broadcast round to 250 clients × 20 streams in
about 160 ms at a third of one CPU core, with delivery p99 under 40 ms. Fan-out **coalesces**:
under load, a connection's undelivered state is replaced by the newest revision per model and the
drain cadence self-clocks to send capacity, so intermediate revisions collapse instead of queueing
— every client still converges on the final revision of every stream, sooner.

## Latency and completion time

| WebSocket clients | Streams per client | Active subscriptions | Fleet completion median | Delivery p50 | Delivery p95 | Delivery p99 | Delivery max |
| ----------------: | -----------------: | -------------------: | ----------------------: | -----------: | -----------: | -----------: | -----------: |
|                10 |                  1 |                   10 |                  115 ms |       1.4 ms |   2.6–2.9 ms |   9.0–9.5 ms |   9.0–9.5 ms |
|               100 |                 10 |                1,000 |              159–162 ms |   4.7–5.6 ms |   12.5–17 ms |   15.7–33 ms |   16.5–36 ms |
|               250 |                 20 |                5,000 |              159–168 ms |   16.5–17 ms |   29.6–32 ms |     37–40 ms |     39–41 ms |

Ranges span two clean runs. Fleet completion measures the full round: publish 50 updates to every
stream at 2 ms intervals, then wait until every client has received the final revision of every
stream. Delivery latency measures individual patches from the server timestamp to receipt by a
client; because only the newest revision of a model ships, every delivered patch is fresh.

## Server resource use

| WebSocket clients |        CPU |          RSS | Event-loop lag p99 | Revisions delivered |
| ----------------: | ---------: | -----------: | -----------------: | ------------------: |
|                10 |   2.7–3.9% | 67.0–67.1 MB |         0.5–0.7 ms |          22.5–23.0% |
|               100 |      28.1% | 74.8–74.9 MB |         4.4–5.9 ms |          15.5–17.0% |
|               250 | 31.5–31.9% | 86.1–86.2 MB |       17.9–20.1 ms |                5.0% |

CPU is process CPU use, where 100% represents one fully occupied logical core. Revisions delivered
below 100% is the coalescing design working, not data loss: transports synchronizes *state*, so a
connection's undelivered patch for a model is replaced by a newer revision, and the autosync drain
cadence stretches to match send capacity (`interval` is the floor, `max_interval` the ceiling).
Every client still receives the final revision of every stream — the completion times above are
the proof.

## Session budgets

The session-cost benchmark (`benchmarks/test_session_cost.py`) measures what a connected-but-idle
session costs the server, and both benchmarks derive the budget metrics below. Measured on the
recording machine at 1,000 clients × 10 streams:

| Budget                                  |   Target |     Measured |
| --------------------------------------- | -------: | -----------: |
| RSS per idle session                    | < 100 KB |      73.7 KB |
| Idle server CPU, whole fleet connected  |     < 5% |         0.5% |
| CPU per state fan-out to 1k subscribers |   < 1 ms | 0.16–0.19 ms |
| Thread growth across 1,000 connects     |        0 |            0 |

Thread flatness is asserted unconditionally on every run — a session must never cost a thread. The
absolute budgets are opt-in (`TRANSPORTS_BUDGETS=1`) because hosted CI runners are too noisy for
stable thresholds; run them locally or on the recording machine. The fan-out CPU budget applies
only at 250+ sessions (`TRANSPORTS_BUDGET_FANOUT_MIN_SESSIONS`) where fixed per-publish cost
amortizes; with coalescing it passes with roughly 5× headroom.

## Event loop comparison

The harness can swap the server's event loop (`TRANSPORTS_BENCH_LOOP=asyncio|uvloop|rsloop`; the
client fleet always stays on asyncio so only the measured process varies). At 1,000 clients × 10
streams on the recording machine, ranges over two runs each:

| Loop    | Fleet completion | Delivery p50 | Delivery p99 | CPU per 1k-subscriber fan-out | RSS per idle session |
| ------- | ---------------: | -----------: | -----------: | ----------------------------: | -------------------: |
| asyncio |       339–345 ms |     63–68 ms |   184–192 ms |                  0.18–0.19 ms |              73.7 KB |
| uvloop  |       264–274 ms |     83–92 ms |   128–137 ms |                  0.18–0.19 ms |              73.2 KB |
| rsloop  |       215–216 ms |     80–83 ms |   219–224 ms |                       0.21 ms |               124 KB |

With coalescing in place the loops converge on CPU. uvloop has the best delivery p99; rsloop the
fastest fleet completion, but ~15% more fan-out CPU and ~70% more memory per idle session — the
latter over the session budget. asyncio remains the default; uvloop is a reasonable choice where
tail latency matters most. Re-measure on the deployment platform — these standings shift between
macOS and Linux.

## Permessage-deflate

WebSocket permessage-deflate compresses each frame **per connection** — encode-once sharing stops
at the compression extension — and its zlib context dominates idle-session memory. At 1,000 × 10
with deflate disabled on the server (`TRANSPORTS_BENCH_WS_DEFLATE=0`, i.e. uvicorn's
`ws_per_message_deflate=False`):

| Configuration | Fleet completion | Delivery p50 | Delivery p99 | CPU per 1k-subscriber fan-out | RSS per idle session |
| ------------- | ---------------: | -----------: | -----------: | ----------------------------: | -------------------: |
| deflate on    |       339–345 ms |     63–68 ms |   184–192 ms |                  0.18–0.19 ms |              73.7 KB |
| deflate off   |           260 ms |        51 ms |       110 ms |                       0.15 ms |          **26.8 KB** |

Disabling deflate improves every measured axis on loopback, and cuts idle memory per session
2.7× — the single most effective configuration change for a high-fan-out deployment where
bandwidth is cheaper than CPU and RAM. Keep it enabled where the network is the constraint.

## Recorded workload

These ranges come from two clean runs recorded on August 27, 2026:

- transports at commit `fa8d8e9` (0.8.2 plus the coalescing fan-out fix)
- one server process and all clients on the same machine over loopback
- Apple silicon (`arm64`, 18 logical CPUs, 64 GiB RAM), macOS 25.5
- CPython 3.12.13, JSON codec, unbatched frames, permessage-deflate on, asyncio loop
- 50 updates per stream per round, published every 2 ms
- one warm-up round followed by three measured rounds

This is a single-machine fan-out baseline, not a capacity limit. It excludes network latency and does
not predict cross-region or multi-host performance. Fleet completion also includes the approximately
100 ms update-publishing interval, so it is broader than patch delivery latency.

An earlier recorded baseline (and the loop comparison it fed) was measured against a stale
installed 0.8.0 wheel rather than the repository code and has been replaced; treat any numbers
predating this page's tables as unreliable.

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
