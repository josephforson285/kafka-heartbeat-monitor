# Performance

Measured on a single machine running the whole stack — Kafka, PostgreSQL and
Grafana in Docker, producer and consumer on the host. Numbers from
`scripts/demo_failure_modes.sh`, not estimated.

## End-to-end latency

Latency is `ingested_at - event_time`: from the producer stamping the reading to
the row being durable in PostgreSQL. It is only meaningful for rows written while
a consumer was actually running, so the measurement window is bounded to that.

Producer and consumer live together at 200 readings/second:

| Metric | RF=1, single broker | RF=3, `min.insync.replicas=2` |
|---|---|---|
| Rows measured | 3925 | 3925 |
| p50 | 128 ms | **133 ms** |
| p95 | 230 ms | **244 ms** |
| max | 696 ms | 1188 ms |

The right-hand column is what the pipeline runs today. The producer now waits for a
second broker to hold the write before it is acknowledged, rather than only the
leader — but across five runs the RF=3 p50 landed between 129 ms and 134 ms against
128 ms unreplicated, which is inside run-to-run variation rather than a measurable
cost. At this rate the consumer's batching dominates; replication would start to show
at throughputs where the network round trip is no longer hidden by it.

Read p50 and p95, not max. `max` is a single worst sample and swung between 530 ms
and 1188 ms across the same five runs — one slow batch, a container scheduling
hiccup, and it moves. The percentiles barely shifted.

Most of the p50 is the consumer's own batching: it waits up to
`batch_timeout_seconds: 2.0` to fill a batch of 500, so a reading arriving early in
a window waits for the rest. Lowering the batch size trades throughput for latency.

A latency figure measured across a gap where no consumer was running is not a
pipeline measurement — it is how long the message sat in the log. An early run
showed an average of 434 seconds for exactly that reason.

## Throughput

The lab runs at 200 readings/second. The pipeline was ramped until it broke to find
out how much headroom that is.

| Target rate | Producer achieved | Consumer lag at the end | |
|---|---|---|---|
| 200 /s | 196 /s | 0 | keeps up |
| 1,000 /s | 983 /s | 0 | keeps up |
| 4,000 /s | 3,927 /s | 0 | keeps up |
| 8,000 /s | 7,747 /s | 0 | keeps up |
| 16,000 /s | 15,464 /s | 12,321 | falls behind |
| 32,000 /s | 30,715 /s | 145,821 | falls behind |

The producer tracks its target to within about 4% even at 32,000/s, so it is not the
limit. Draining a 139,321-message backlog with nothing else running measures the
consumer directly: **9,753 messages/second sustained**, which is where the ramp turns
over. That is roughly 50× the workload this project actually carries.

### Where the limit is, and why it is the right one

Neither obvious suspect is responsible. PostgreSQL writes 34,538 rows/second through
the same `write_batch` at the configured batch size, and parsing plus classification
runs at 297,528 messages/second. Both are far above the pipeline.

The cost is `consumer.commit(asynchronous=False)` — one network round trip to the
group coordinator per batch. Varying only the batch size, against an identical
489,321-message backlog:

| `batch_size` | Throughput | Commits performed |
|---|---|---|
| 500 | 14,521 msg/s | 978 |
| 2,000 | 25,725 msg/s | 244 |
| 5,000 | 31,792 msg/s | 97 |

Throughput more than doubles as the commit count falls tenfold, and at 5,000 it
converges on the 34,538 rows/second PostgreSQL can absorb — the database becomes the
limit only once the commits stop being one.

So the bottleneck is the safety mechanism. Committing synchronously after the write
is what makes a crash replay rather than lose, and it is also what caps throughput.
`batch_size` is the dial between them: a larger batch amortises the commit over more
rows and replays more of them after a crash.

500 stays the default. It gives 14,521 messages/second against a workload of 200,
and a crash costs at most 500 replayed rows that the primary key absorbs for free.
Buying throughput this project does not need, at the price of a longer replay, would
be the wrong trade.

## Data profile

19,623 readings and 382 rejects accumulated across the demo runs.

| Classification | Count | Share |
|---|---|---|
| normal | 15,580 | 79.4% |
| bradycardia | 3,502 | 17.8% |
| tachycardia | 293 | 1.5% |
| critical | 248 | 1.3% |

Bradycardia is over-represented against the configured `anomaly_rate` of 5% because
the random walk starts each customer at a baseline between 58 and 88 bpm, so
customers at the low end drift below 60 naturally. That is realistic — a resting
heart rate in the 50s is common — rather than a fault.

Rejects break down as 377 sensor faults (impossible values from the generator's
`fault_rate: 0.02`) and 5 contract violations (the payloads injected deliberately
by proof 3):

| Reason | Count |
|---|---|
| heart_rate outside plausible range 20–250 | 377 |
| not valid JSON | 2 |
| missing field(s) | 1 |
| heart_rate must be an integer | 1 |
| event_id is not a UUID | 1 |

## Storage

`heartbeat_readings` is 4984 kB at 19,623 rows including its four indexes — roughly
260 bytes per row, of which the row itself is about half. At 200 readings/second
that is about 4.6 GB per year of raw growth before any partitioning or retention
policy.

## Indexes

Checked with `EXPLAIN` against 19,623 rows rather than assumed.

| Index | Size | Serves |
|---|---|---|
| `heartbeat_readings_pkey` | 792 kB | the `ON CONFLICT` dedup lookup |
| `idx_readings_customer_time` | 1240 kB | one patient over a window |
| `idx_readings_time` | 672 kB | fleet-wide time range |
| `idx_readings_alerting` | 168 kB | the abnormal rows only |

The primary key had **27,480 scans** — one per insert attempt. That is the dedup
mechanism working: every row inserted asks whether its `event_id` is already there.
It is the busiest index in the schema and the reason the guarantee costs so little.

`idx_readings_customer_time` is confirmed used — the customer-and-window query plans
as a bitmap index scan on it. Note it only comes into play when the dashboard's
customer variable is narrowed; the default "All" view does not filter by customer.

`idx_readings_time` looks unused at first glance, because a 15-minute window covers
most of a table holding only a few hours of data, and a sequential scan is genuinely
cheaper there. That is the planner being right, not the index being wrong: over a
5-second window it plans an index-only scan, which is what every window looks like
once the table holds more than a few hours.

`idx_readings_alerting` is partial — it indexes only `hr_class <> 'normal'`, which is
why it is 168 kB against the 672 kB of the full time index while serving the alert
panels just as well.

## Configuration that drives these numbers

| Setting | Value | Effect |
|---|---|---|
| `consumer.batch_size` | 500 | rows per transaction, and per offset commit — the throughput dial |
| `consumer.batch_timeout_seconds` | 2.0 | upper bound on how long a batch waits to fill |
| `producer.linger.ms` | 20 | producer-side batching before a send |
| `producer.acks` | all | waits for the in-sync replicas before acknowledging |
| `replication_factor` | 3 | copies of each partition, on three brokers |
| `min_insync_replicas` | 2 | copies required before a write is acknowledged |
| `producer.compression.type` | snappy | trades CPU for network and disk |
| `session.timeout.ms` | 10000 | how long a dead consumer holds its partitions |

`acks=all` costs latency and is not negotiable here: acknowledging before the write
is replicated would mean a broker failure could silently lose readings.
