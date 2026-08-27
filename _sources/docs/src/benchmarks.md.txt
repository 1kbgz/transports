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
