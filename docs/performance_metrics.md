# Performance

Measured on a single machine running the whole stack — Kafka, PostgreSQL and
Grafana in Docker, producer and consumer on the host. Numbers from
`scripts/demo_failure_modes.sh`, not estimated.

## End-to-end latency

Latency is `ingested_at - event_time`: from the producer stamping the reading to
the row being durable in PostgreSQL. It is only meaningful for rows written while
a consumer was actually running, so the measurement window is bounded to that.

Producer and consumer live together at 200 readings/second:

| Metric | Value |
|---|---|
| Rows measured | 3925 |
| p50 | 128 ms |
| p95 | 230 ms |
| max | 696 ms |

Most of the p50 is the consumer's own batching: it waits up to
`batch_timeout_seconds: 2.0` to fill a batch of 500, so a reading arriving early in
a window waits for the rest. Lowering the batch size trades throughput for latency.

A latency figure measured across a gap where no consumer was running is not a
pipeline measurement — it is how long the message sat in the log. An early run
showed an average of 434 seconds for exactly that reason.

## Throughput

The producer paces itself against a fixed clock, so its rate is whatever `--rate`
asks for. The consumer was never the bottleneck at these rates:

| Run | Result |
|---|---|
| 2000 messages, backlog drain | consumed in ~6 s (~330 msg/s) |
| 200 msg/s sustained, two consumers | lag 0–2 per partition |
| 7500 messages, single consumer | consumed in ~20 s |

Lag stayed at 0–2 messages per partition throughout the two-consumer run, meaning
the consumers were keeping pace with the producer rather than falling behind.

## Data profile

12,599 readings and 243 rejects accumulated across the demo runs.

| Classification | Count | Share |
|---|---|---|
| normal | 10,069 | 79.9% |
| bradycardia | 2,205 | 17.5% |
| tachycardia | 198 | 1.6% |
| critical | 127 | 1.0% |

Bradycardia is over-represented against the configured `anomaly_rate` of 5% because
the random walk starts each customer at a baseline between 58 and 88 bpm, so
customers at the low end drift below 60 naturally. That is realistic — a resting
heart rate in the 50s is common — rather than a fault.

Rejects break down as 238 sensor faults (impossible values from the generator's
`fault_rate: 0.02`) and 5 contract violations (the payloads injected deliberately
by proof 3):

| Reason | Count |
|---|---|
| heart_rate outside plausible range 20–250 | 238 |
| not valid JSON | 2 |
| missing field(s) | 1 |
| heart_rate must be an integer | 1 |
| event_id is not a UUID | 1 |

## Storage

`heartbeat_readings` is 3296 kB at 12,599 rows including its four indexes — roughly
270 bytes per row, of which the row itself is about half. At 200 readings/second
that is about 4.6 GB per year of raw growth before any partitioning or retention
policy.

## Configuration that drives these numbers

| Setting | Value | Effect |
|---|---|---|
| `consumer.batch_size` | 500 | rows per transaction |
| `consumer.batch_timeout_seconds` | 2.0 | upper bound on how long a batch waits to fill |
| `producer.linger.ms` | 20 | producer-side batching before a send |
| `producer.acks` | all | waits for the in-sync replicas before acknowledging |
| `producer.compression.type` | snappy | trades CPU for network and disk |
| `session.timeout.ms` | 10000 | how long a dead consumer holds its partitions |

`acks=all` costs latency and is not negotiable here: acknowledging before the write
is replicated would mean a broker failure could silently lose readings.
