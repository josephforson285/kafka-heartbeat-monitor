# Performance

Measured on one machine — Kafka, PostgreSQL and Grafana in Docker, producer and
consumer on the host. 

## End-to-end latency

Latency is measured as:

`ingested_at - event_time`

at 200 readings/second.

| Metric | RF=1, single broker | RF=3, `min.insync.replicas=2` |
|---|---|---|
| Rows measured | 3925 | 3925 |
| p50 | 128 ms | **134 ms** |
| p95 | 230 ms | **248 ms** |
| max | 696 ms | 1206 ms |

 

p50 and p95 are more representative than `max`, which varied heavily between runs.

Most latency comes from `batch_timeout_seconds: 2.0`.

Messages waiting in Kafka while no consumer is running are excluded from pipeline latency.

## Throughput

The system was ramped beyond normal load to find its limit.

|   Target | Achieved | Final lag | Result       |
| -------: | -------: | --------: | ------------ |
|    200/s |    196/s |         0 | keeps up     |
|  1,000/s |    983/s |         0 | keeps up     |
|  4,000/s |  3,927/s |         0 | keeps up     |
|  8,000/s |  7,747/s |         0 | keeps up     |
| 16,000/s | 15,464/s |    12,321 | falls behind |
| 32,000/s | 30,715/s |   145,821 | falls behind |

Draining a 139,321-message backlog measured consumer throughput at **9,753 msg/s sustained**.


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

<!-- Bradycardia exceeds the configured `anomaly_rate` of 5% because the random walk
starts each customer between 58 and 88 bpm, so low-baseline customers drift below 60
naturally — realistic rather than a fault. The 377 sensor faults come from
`fault_rate: 0.02`; the 5 contract violations were injected by proof 3. -->

## Storage

`heartbeat_readings` uses about **4.9 MB** for 19,623 rows and four indexes, roughly **260 bytes per row**.


## Indexes

Checked with `EXPLAIN`, not assumed.

| Index | Size | Serves |
|---|---|---|
| `heartbeat_readings_pkey` | 792 kB | the `ON CONFLICT` dedup lookup |
| `idx_readings_customer_time` | 1240 kB | one patient over a window |
| `idx_readings_time` | 672 kB | fleet-wide time range |
| `idx_readings_alerting` | 168 kB | abnormal rows only |

The primary key recorded **27,480 scans**, showing the deduplication path being actively used.




## Configuration

| Setting                          | Value  |
| -------------------------------- | ------ |
| `consumer.batch_size`            | 500    |
| `consumer.batch_timeout_seconds` | 2.0    |
| `producer.linger.ms`             | 20     |
| `producer.acks`                  | all    |
| `replication_factor`             | 3      |
| `min_insync_replicas`            | 2      |
| `producer.compression.type`      | snappy |
| `session.timeout.ms`             | 10000  |

`acks=all` adds latency, but ensures writes are replicated before acknowledgement.
