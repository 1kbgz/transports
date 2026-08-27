# Server fan-out benchmark results

The recorded baseline keeps WebSocket delivery below 25 ms at p95 with 250 concurrent clients,
each receiving 20 state streams. At that load, all 5,000 client-stream subscriptions reach the final
revision in 333 ms while the server uses 66.5% CPU and 87.1 MB RSS.

## Latency and completion time

| WebSocket clients | Streams per client | Active subscriptions | Fleet completion (median) | Delivery p50 | Delivery p95 | Delivery p99 | Delivery max |
| ----------------: | -----------------: | -------------------: | ------------------------: | -----------: | -----------: | -----------: | -----------: |
|                10 |                  1 |                   10 |                    136 ms |       1.1 ms |       5.4 ms |      10.6 ms |      10.6 ms |
|               100 |                 10 |                1,000 |                    162 ms |       3.7 ms |      10.3 ms |      13.6 ms |      14.9 ms |
|               250 |                 20 |                5,000 |                    333 ms |      13.7 ms |      24.3 ms |      27.3 ms |      31.7 ms |

Fleet completion measures the full round: publish 50 updates to every stream at 2 ms intervals, then
wait until every client has received the final revision of every stream. Delivery latency measures
individual patches from the server timestamp to receipt by a client.

## Server resource use

| WebSocket clients |   CPU |     RSS | Event-loop lag p99 | Revisions delivered |
| ----------------: | ----: | ------: | -----------------: | ------------------: |
|                10 |  2.6% | 66.8 MB |             1.3 ms |               24.5% |
|               100 | 31.9% | 74.7 MB |             5.1 ms |               21.5% |
|               250 | 66.5% | 87.1 MB |            21.2 ms |               21.5% |

CPU is process CPU use, where 100% represents one fully occupied logical core. Revisions delivered
below 100% reflect expected coalescing, not data loss: transports synchronizes state, so its 10 ms
autosync window can replace intermediate revisions while every client still receives the final one.

## Recorded workload

These numbers come from the latest recorded run for each load on August 27, 2026:

- transports 0.8.0 at commit `117e3d9`
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
:view: overview
:metric: median
:selector: latest-per-benchmark
```

The [full interactive report](https://1kbgz.github.io/transports/benchmarks/) includes every recorded
run and filter.
