# Real-Time Customer Heart Beat Monitoring

[![CI](https://github.com/josephforson285/kafka-heartbeat-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/josephforson285/kafka-heartbeat-monitor/actions/workflows/ci.yml)

A streaming pipeline that simulates heart rate sensors, moves the readings through
Apache Kafka, validates and classifies them, stores them in PostgreSQL, and shows
them on a Grafana dashboard with an alert for patients in a critical range.

![architecture](docs/architecture.svg)

```mermaid
flowchart LR
    G["Generator<br/>50 customers"] --> P["Producer<br/>acks=all, idempotent<br/>key = customer_id"]
    P --> T[("heartbeat.raw<br/>3 partitions × 3 replicas<br/>brokers 1, 2, 3")]
    T --> C["Consumer group<br/>manual offsets<br/>validate → classify"]
    C -->|valid| R[("heartbeat_readings")]
    C -->|rejected| X[("heartbeat_rejects")]
    C -.->|replay copy| D[("heartbeat.dlq")]
    R --> GF["Grafana"]
    X --> GF
```

## Quick start

Requires Docker and Python 3.11+.

```bash
cp .env.example .env          # then edit the passwords
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

docker compose up -d          # Kafka, PostgreSQL, Grafana
.venv/bin/heartbeat create-topics
```

The schema is applied automatically when PostgreSQL first starts. Run
`heartbeat init-db` to apply it to a database that already exists — it is
idempotent either way.

Then, in two terminals:

```bash
.venv/bin/heartbeat produce      # stream synthetic readings into Kafka
.venv/bin/heartbeat consume      # write them into PostgreSQL
```

Open Grafana at <http://localhost:3000> and the dashboard is already there.

| Service | Port | Notes |
|---|---|---|
| Kafka | 9092, 9094, 9096 | three brokers, KRaft, no ZooKeeper |
| PostgreSQL | 5434 | 5432 and 5433 avoid clashing with other local databases |
| Grafana | 3000 | credentials from `.env` |

Upgrading an existing stack: the broker service was renamed `kafka` → `kafka1`, so
`docker compose down -v` leaves the old container behind holding port 9092. Use
`docker compose down -v --remove-orphans`. The old `heartbeat_kafka_data` volume is
left behind for the same reason — it is unused, and
`docker volume rm heartbeat_kafka_data` reclaims the space.

## Commands

```
heartbeat init-db                        apply the database schema
heartbeat create-topics                  create the topics, or verify they match config
heartbeat topic-info                     leader and in-sync replicas per partition
heartbeat produce [--count N] [--duration S] [--rate R]
heartbeat consume [--group ID] [--max-messages N] [--drain]
```

Every stage is re-runnable; running any of them twice is a no-op the second time.
`--drain` stops the consumer once it is caught up, which is what the tests and the
demo script use.

## Design decisions

### The brief says ZooKeeper; this uses KRaft

Apache Kafka 4 removed ZooKeeper. The broker is the official `apache/kafka` image
running in KRaft mode, which is the only mode that still exists. `confluent-kafka`
is the Python client library — the broker is Apache Kafka either way, in the same
sense that `psycopg` is a driver and PostgreSQL is the database.

### The message carries an `event_id` the brief does not mention

The brief lists `customer_id`, `timestamp` and `heart_rate`. With only those three
fields there is no way to tell a redelivered message from a new one, and Kafka
redelivers whenever a consumer dies between processing and committing.

So the producer stamps a UUID, `event_id` is the primary key of
`heartbeat_readings`, and inserts use `ON CONFLICT DO NOTHING`. At-least-once
delivery becomes effectively-once *storage*. This — not Kafka transactions — is
the answer to "how do you get exactly-once?"

### Messages are keyed by `customer_id`

Kafka only guarantees ordering within a partition. Keying by customer sends one
patient's readings to one partition, so their readings stay in order. Unkeyed,
a patient's 11:00:03 reading could be processed before their 11:00:01 one, which
for time-series vitals is a defect.

The topic has three partitions so that a consumer group can hold more than one
member and actually rebalance. Partition count is effectively one-way: raising it
later changes which partition a key maps to and breaks that ordering guarantee for
existing keys.

### Invalid and alerting are different things

The brief says the consumer should check for "heart rate too high/low" and implies
filtering those out. That is backwards for a monitoring system:

- **190 bpm** is a real reading from a patient who needs attention. Stored, and
  classified `critical`. Dropping it would discard the event the system exists to catch.
- **900 bpm** is not a possible human heart rate. That is a broken sensor, and it
  goes to `heartbeat_rejects` with the reason attached.

Contract violations — malformed JSON, missing fields, a non-UUID `event_id`, a
timestamp with no offset — take the same path as sensor faults. Nothing is silently
discarded; every rejection is queryable with the partition and offset it came from.

### Three brokers, so replication is real

Replication factor is how many *different* brokers hold a copy of a partition, so
RF=3 is impossible on one broker — Kafka refuses the topic outright. On a single
broker `acks=all` is also nearly meaningless: "all in-sync replicas" is one machine,
the same one `acks=1` would have used.

With three brokers, `replication_factor: 3` and `min_insync_replicas: 2`, a write is
only acknowledged once two brokers hold it, and one broker can die without stopping
ingestion. Two brokers would not help: RF=2 with `min.insync.replicas=2` stops writes
when either dies, and with 1 there is no guarantee left. Three is the smallest cluster
where failure is survivable — the same reason KRaft's controller quorum needs three
voters to tolerate losing one.

The internal offsets and transaction-state topics are replicated too. Leaving those
at 1 while the data topic is replicated survives a broker loss but forgets where the
consumer had got to.

### Topics are never created by accident

`auto.create.topics.enable` is off. Left on — as it is by default — producing to a
mistyped topic silently creates it at the broker defaults: one partition, no
replication. Real data then lands in a topic nobody configured.

`heartbeat create-topics` also compares topics that already exist against the config
and refuses to continue on a mismatch, rather than reporting "already exists" and
moving on. This was not hypothetical: an auto-created `heartbeat.dlq` sat at RF=1
while the config claimed 3, and the old existence-only check said nothing.

### Offsets are committed after the write, never before

Readings and rejects for one poll go into PostgreSQL in a single transaction. Only
once that transaction lands does the consumer commit its offsets. A crash in between
replays the batch rather than losing it, and the primary key absorbs the replay.

Auto-commit is off. With it on, offsets advance on a timer whether or not the rows
were ever written.

### The dead-letter topic is a convenience, not the record of truth

`heartbeat_rejects` is written inside the transaction. The copy forwarded to
`heartbeat.dlq` happens afterwards and is best-effort, so that a failed produce can
never mean data loss. The topic exists so a fixed consumer can replay poison messages
without re-reading the whole log.

### Grafana is provisioned from files

The datasource, dashboard and alert rule are YAML and JSON in `docker/grafana/`,
mounted into the container. Configuring Grafana through its UI would put that state
in a Docker volume, and a fresh clone would come up with an empty Grafana.

### Deliberately not included

Schema Registry and Avro (a shared schema module and strict parsing are proportionate
here), Prometheus and a Kafka metrics exporter (consumer lag is one CLI command), and
Spark (the same topic could feed a windowed aggregation, but the per-record path does
not need it). Reaching for those at this scale would be over-engineering.

## Failure modes

`scripts/demo_failure_modes.sh` proves three of them and asserts the results rather
than describing them. It resets the stack, so run it deliberately.

**A consumer killed mid-stream loses nothing and duplicates nothing.** The consumer
is `SIGKILL`ed while the producer is still running, then restarted:

```
killed with SIGKILL after 677 of 2000 rows were written
recovery: consumed=1313 inserted=1288 duplicates=0 rejected=25
PASS  no records lost (2000)
PASS  no records duplicated (1965)
```

**A second consumer joining the group triggers a rebalance.**

```
A: assigned [0,1,2] → revoked [0,1,2] → assigned [0,1]
B: assigned [2]
```

**Malformed messages are recorded and skipped.** Five deliberately broken payloads:

```
not valid JSON: Expecting value: line 1 column 1 (char 0)
heart_rate must be an integer, got 'seventy'
event_id is not a UUID: 'not-a-uuid'
missing field(s): event_id, event_time, heart_rate
not valid JSON  (invalid UTF-8)
```

**A broker can die mid-stream without stopping ingestion.** `kafka2` is stopped while
the producer and consumer are running:

```
before      partition 2   leader 2   replicas [1,2,3]   isr [1,2,3]
one down    partition 2   leader 3   replicas [1,2,3]   isr [1,3]   UNDER-REPLICATED, missing [2]
recovered   partition 2   leader 3   replicas [1,2,3]   isr [1,2,3]

PASS  ingestion continued through the broker failure (7832 -> 10533 rows)
PASS  in-sync replicas degraded while it was down (4)
PASS  in-sync replicas recovered (0)
PASS  still no duplicates (19623)
```

Leadership moved off the dead broker on its own and the pipeline kept writing —
`min.insync.replicas: 2` was still satisfied by the two survivors. Note that
leadership stays on broker 3 after recovery; Kafka does not move it back
automatically.

The figures above are from the run whose logs are committed in
[docs/sample_output/](docs/sample_output/); re-running the script regenerates both
together. Alongside them is
[database-contents.txt](docs/sample_output/database-contents.txt) — the table
definition, the classification breakdown, recent rows with the partition and offset
they came from, and the count that matters:

```
 total_readings | distinct_event_ids
----------------+--------------------
           5890 |               5890
```

**The database going away is survivable too.** Stopping PostgreSQL mid-consume leaves
the group cleanly and exits 3 with one readable line; restarting it and re-running
`consume --drain` picks up the uncommitted batch with `duplicates=0`.

Consumer lag, at any time. Note the internal listener: run inside a broker container,
the external addresses advertise `localhost:909x`, which from in there resolves only
to that same container, so any call needing a second broker times out.

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:19092 --describe --group heartbeat-writer
```

## Performance

At 200 readings/second with producer and consumer running together, measured as
`ingested_at - event_time` over rows written during that window:

| p50 | p95 | max | rows |
|---|---|---|---|
| 133 ms | 244 ms | 1188 ms | 3925 |

See [docs/performance_metrics.md](docs/performance_metrics.md).

## Tests

```bash
.venv/bin/python -m pytest
```

61 unit tests covering the event contract's rejection paths, the classification band
boundaries either side of every threshold, and configuration loading and validation. They are
pure functions — no broker or database — and run in under a second.

Eleven more test the guarantees that live in the DDL rather than in Python: that
`ON CONFLICT` really does suppress a replayed batch, that a batch failing a `CHECK`
constraint rolls back completely, and that a reject payload which is not valid UTF-8
can still be stored. Those truncate their tables, so they are skipped unless you opt
in against a disposable database:

```bash
HEARTBEAT_TEST_DSN="host=localhost port=5434 dbname=heartbeat_test user=heartbeat password=..." \
HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS=1 .venv/bin/python -m pytest
```

The demo script's reset (`docker compose down -v`) drops this database along with
everything else, so recreate it when you have just run the proofs:

```bash
docker compose exec postgres psql -U heartbeat -d postgres -c "CREATE DATABASE heartbeat_test;"
```

CI runs all 72 against a real PostgreSQL service, then brings up this repository's
own `docker-compose.yml` and runs `scripts/demo_failure_modes.sh` against it. So the
failure-mode claims below are re-proven on every push rather than asserted once —
and the logs are attached to each run as a build artifact.

## Layout

```
config/config.yaml            parameters (secrets live in .env)
sql/001_schema.sql            tables, constraints, indexes
src/heartbeat/
  schema.py                   the event contract, shape and parsing only
  validation.py               plausibility and clinical classification
  generator.py                synthetic readings
  producer.py                 Kafka producer and its run loop
  consumer.py                 poll, validate, write, then commit
  db.py                       transactional batch upsert
  config.py                   typed config loading and validation
  logging_conf.py             logging setup
  runtime.py                  cooperative shutdown
  cli.py                      the single entrypoint
docker/grafana/               provisioned datasource, dashboard, alert
scripts/demo_failure_modes.sh the proofs
docs/                         diagram, metrics, test plan, sample output
tests/                        unit tests
```

## What would change in production

Replication is already what production would run — three brokers, `replication.factor=3`,
`min.insync.replicas=2`, `acks=all`. What is still missing is everything around it:

- **The brokers share a machine.** Three containers on one host survive a broker
  process dying, which is what proof 4 shows, but not the host dying. Real clusters
  spread brokers across failure domains.
- **No TLS or SASL.** Every listener is plaintext and unauthenticated.
- **No Schema Registry.** A shared Python module is enough while one team owns both
  ends of the topic. It stops being enough the moment someone else produces.
- **`heartbeat_readings` is a single table.** At sustained volume it wants
  time-based partitioning and a retention policy.
- **Broker metrics are read off the CLI.** Production exports JMX to Prometheus and
  alerts on under-replicated partitions rather than looking at `topic-info` by hand.
