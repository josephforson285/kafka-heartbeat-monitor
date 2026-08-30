# Performance

Measured on one machine — Kafka, PostgreSQL and Grafana in Docker, producer and
consumer on the host. Numbers from `scripts/demo_failure_modes.sh`, not estimated.

## End-to-end latency

`ingested_at - event_time`: producer stamping the reading to the row being durable.
Producer and consumer running together at 200 readings/second:

| Metric | RF=1, single broker | RF=3, `min.insync.replicas=2` |
|---|---|---|
| Rows measured | 3925 | 3925 |
| p50 | 128 ms | **134 ms** |
| p95 | 230 ms | **248 ms** |
| max | 696 ms | 1206 ms |

The right column is what runs today. Across five runs the RF=3 p50 landed between
129 and 134 ms against 128 unreplicated — run-to-run variation, not a measurable cost
of replication. At this rate the consumer's batching dominates; replication would
show at throughputs where the network round trip is no longer hidden by it.

**Read p50 and p95, not max.** `max` is a single worst sample and swung between
530 ms and 1206 ms across those same runs while the percentiles barely moved.

Most of the p50 is `batch_timeout_seconds: 2.0` — a reading arriving early in a
window waits for the rest of the batch.

**A figure measured across a gap where no consumer was running is not a pipeline
measurement**, it is how long the message sat in the log. An early run showed an
average of 434 seconds for exactly that reason.

## Throughput

Ramped until it broke, to find the headroom over the 200/s the lab runs at:

| Target | Producer achieved | Lag at end | |
|---|---|---|---|
| 200 /s | 196 /s | 0 | keeps up |
| 1,000 /s | 983 /s | 0 | keeps up |
| 4,000 /s | 3,927 /s | 0 | keeps up |
| 8,000 /s | 7,747 /s | 0 | keeps up |
| 16,000 /s | 15,464 /s | 12,321 | falls behind |
| 32,000 /s | 30,715 /s | 145,821 | falls behind |

The producer tracks its target to within 4% even at 32,000/s, so it is not the limit.
Draining a 139,321-message backlog measures the consumer directly: **9,753 msg/s
sustained** — roughly 50× this workload.

## Data profile

19,623 readings and 382 rejects across the demo runs.

| Classification | Count | Share |
|---|---|---|
| normal | 15,580 | 79.4% |
| bradycardia | 3,502 | 17.8% |
| tachycardia | 293 | 1.5% |
| critical | 248 | 1.3% |

| Reject reason | Count |
|---|---|
| heart_rate outside plausible range 20–250 | 377 |
| not valid JSON | 2 |
| missing field(s) | 1 |
| heart_rate must be an integer | 1 |
| event_id is not a UUID | 1 |

Bradycardia exceeds the configured `anomaly_rate` of 5% because the random walk
starts each customer between 58 and 88 bpm, so low-baseline customers drift below 60
naturally — realistic rather than a fault. The 377 sensor faults come from
`fault_rate: 0.02`; the 5 contract violations were injected by proof 3.

## Storage

`heartbeat_readings` is 4984 kB at 19,623 rows including four indexes — about 260
bytes per row, half of it index. At 200 readings/second that is roughly **4.6 GB per
year** before partitioning or retention.

## Indexes

Checked with `EXPLAIN`, not assumed.

| Index | Size | Serves |
|---|---|---|
| `heartbeat_readings_pkey` | 792 kB | the `ON CONFLICT` dedup lookup |
| `idx_readings_customer_time` | 1240 kB | one patient over a window |
| `idx_readings_time` | 672 kB | fleet-wide time range |
| `idx_readings_alerting` | 168 kB | abnormal rows only |

The primary key showed **27,480 scans — one per insert attempt.** That is the dedup
mechanism working, and the reason the guarantee costs so little.

`idx_readings_customer_time` plans as a bitmap index scan, but only when the
dashboard's patient selector is narrowed.

`idx_readings_time` appears unused because a 15-minute window covers most of a table
holding a few hours, where a sequential scan is genuinely cheaper — the planner being
right, not the index being wrong. Over a 5-second window it plans an index-only scan,
which is what every window looks like on a larger table.

`idx_readings_alerting` is partial, indexing only `hr_class <> 'normal'` — 168 kB
against 672 kB for the full time index, serving the alert panels just as well.

## Configuration behind these numbers

| Setting | Value | Effect |
|---|---|---|
| `consumer.batch_size` | 500 | rows per transaction and per commit — the throughput dial |
| `consumer.batch_timeout_seconds` | 2.0 | upper bound on how long a batch waits to fill |
| `producer.linger.ms` | 20 | producer-side batching before a send |
| `producer.acks` | all | waits for the in-sync replicas |
| `replication_factor` | 3 | copies of each partition |
| `min_insync_replicas` | 2 | copies required before acknowledgement |
| `producer.compression.type` | snappy | trades CPU for network and disk |
| `session.timeout.ms` | 10000 | how long a dead consumer holds its partitions |

`acks=all` costs latency and is not negotiable: acknowledging before the write is
replicated would let a broker failure lose readings silently.
