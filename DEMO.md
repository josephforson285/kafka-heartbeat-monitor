# Demo walkthrough

Every command here has been run against this repository. Expected output is shown
so you can tell at a glance whether a step worked.

Allow about 15 minutes for the full walkthrough, or jump to
[section 6](#6-the-four-proofs) if you only want the failure-mode evidence.

---

## 1. Setup

Needs Docker and Python 3.11 or newer.

```bash
git clone git@github.com:josephforson285/kafka-heartbeat-monitor.git
cd kafka-heartbeat-monitor

cp .env.example .env        # then change the two passwords
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Start the stack — three Kafka brokers, PostgreSQL and Grafana:

```bash
docker compose up -d
docker compose ps
```

Wait until all five report `healthy`:

```
SERVICE    STATUS
grafana    Up (healthy)
kafka1     Up (healthy)
kafka2     Up (healthy)
kafka3     Up (healthy)
postgres   Up (healthy)
```

| Service | Port |
|---|---|
| Kafka | 9092, 9094, 9096 |
| PostgreSQL | 5434 |
| Grafana | 3000 |

The database schema is applied automatically the first time PostgreSQL starts.
Create the topics:

```bash
.venv/bin/heartbeat create-topics
```

```
INFO  heartbeat.cli  created topic heartbeat.raw
INFO  heartbeat.cli  created topic heartbeat.dlq
```

Run it again — it verifies rather than duplicating:

```
INFO  heartbeat.cli  topic heartbeat.raw already exists and matches config
INFO  heartbeat.cli  topic heartbeat.dlq already exists and matches config
```

---

## 2. Prove the tests pass

```bash
.venv/bin/python -m pytest
```

```
61 passed, 11 skipped
```

The 11 skipped are integration tests that truncate their tables, so they only run
against a database you name explicitly:

```bash
docker compose exec postgres psql -U heartbeat -d postgres -c "CREATE DATABASE heartbeat_test;"

HEARTBEAT_TEST_DSN="host=localhost port=5434 dbname=heartbeat_test user=heartbeat password=YOUR_PASSWORD" \
HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS=1 .venv/bin/python -m pytest
```

```
72 passed
```

---

## 3. Run the pipeline

Two terminals. Producer first:

```bash
.venv/bin/heartbeat produce --rate 50
```

```
INFO  heartbeat.producer  producing to heartbeat.raw at 50.0 readings/s across 50 customers
```

Consumer second:

```bash
.venv/bin/heartbeat consume
```

```
INFO  heartbeat.consumer  consuming heartbeat.raw as group heartbeat-writer
INFO  heartbeat.consumer  partitions assigned: [0, 1, 2]
```

Leave both running. `Ctrl-C` stops either cleanly.

### See the data

```bash
docker compose exec postgres psql -U heartbeat -d heartbeat -c \
  "SELECT hr_class, count(*), min(heart_rate), max(heart_rate)
   FROM heartbeat_readings GROUP BY 1 ORDER BY 2 DESC;"
```

```
  hr_class   | count | min | max
-------------+-------+-----+-----
 normal      |  1576 |  60 | 100
 bradycardia |   340 |  21 |  59
 tachycardia |    32 | 104 | 178
 critical    |    20 | 186 | 235
```

The tachycardia and critical rows are the point: those are **stored**, not filtered
out. A reading of 190 is a patient who needs attention, not bad data.

### See what was rejected, and why

```bash
docker compose exec postgres psql -U heartbeat -d heartbeat -c \
  "SELECT reason, count(*) FROM heartbeat_rejects GROUP BY 1 ORDER BY 2 DESC;"
```

```
 heart_rate 0 outside plausible range 20-250    | 28
 heart_rate 999 outside plausible range 20-250  | 23
 heart_rate -1 outside plausible range 20-250   | 24
```

Impossible values are sensor faults. They are kept with the reason and the exact
partition and offset they came from — never silently dropped.

### The claim that matters

```bash
docker compose exec postgres psql -U heartbeat -d heartbeat -c \
  "SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_ids FROM heartbeat_readings;"
```

```
 rows  | distinct_ids
-------+--------------
 19623 |        19623
```

Equal, always. That is exactly-once storage, and section 6 shows it holding through
a crash.

---

## 4. The dashboard

Open <http://localhost:3000> and log in with the credentials from your `.env`.
The dashboard is already there — nothing to import.

**Heart Beat Monitoring** shows:

- four stat tiles: rows stored, alerting readings, rejected messages, p95 latency
- heart rate per customer, with threshold bands at 60, 100 and 180 bpm
- which patients are currently outside the normal band
- readings by classification over time
- why messages were rejected

With the producer and consumer running it updates every 5 seconds.

### The alert

**Alerting → Alert rules → Patient in critical heart rate range.**

It counts *distinct patients* above the critical threshold in the last 5 minutes,
not readings — one person sustained above it matters more than a burst from one
sensor. Let the pipeline run a minute or two and it moves `Normal → Pending → Firing`.

---

## 5. See the replication

```bash
.venv/bin/heartbeat topic-info
```

```
brokers: [1, 2, 3]

heartbeat.raw
  partition 0   leader 3   replicas [1, 2, 3]   isr [1, 2, 3]
  partition 1   leader 1   replicas [1, 2, 3]   isr [1, 2, 3]
  partition 2   leader 2   replicas [1, 2, 3]   isr [1, 2, 3]
```

Every partition is held by all three brokers. `isr` is the in-sync set — when it
shrinks, a replica has fallen behind or died.

### Consumer lag

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:19092 --describe --group heartbeat-writer
```

```
GROUP             TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
heartbeat-writer  heartbeat.raw  0          449             573             124
```

Note `kafka1:19092`, not `localhost`. Inside a broker container the external
addresses point back at that same container, so anything needing a second broker
times out.

---

## 6. The four proofs

```bash
./scripts/demo_failure_modes.sh
```

**This resets the stack and wipes the database.** Takes about four minutes.

```
══ proof 1 — consumer killed mid-batch
killed with SIGKILL after 322 of 2000 rows were written
PASS  no records lost (2000)
PASS  no records duplicated (1965)

══ proof 2 — a second consumer joins the group
consumer A: assigned [0,1,2] → revoked [0,1,2] → assigned [0,1]
consumer B: assigned [2]

══ proof 3 — malformed messages
PASS  every malformed message recorded (5)

══ proof 4 — a broker fails
PASS  ingestion continued through the broker failure (7822 -> 10254 rows)
PASS  in-sync replicas degraded while it was down (4)
PASS  in-sync replicas recovered (0)
PASS  still no duplicates (19623)

══ all proofs passed
```

Logs land in `docs/sample_output/`.

| Proof | What it kills | What it shows |
|---|---|---|
| 1 | the consumer, `SIGKILL`, mid-stream | the uncommitted batch replays; the primary key absorbs it |
| 2 | nothing — adds a second consumer | partitions redistribute automatically |
| 3 | nothing — injects 5 broken payloads | each rejected by name, pipeline keeps running |
| 4 | `kafka2`, with data flowing | leadership moves, ingestion never stops |

---

## 7. Break it yourself

Each of these was run to produce the results in `docs/test_cases.md`.

### Kill the consumer, restart it

```bash
.venv/bin/heartbeat consume --group demo &
sleep 10
kill -9 %1                                    # SIGKILL — no chance to commit
.venv/bin/heartbeat consume --drain --group demo
```

The restart replays the uncommitted batch and reports `duplicates=N, inserted=0` for
those rows. Nothing lost, nothing double-stored.

Do the same with `kill -TERM` instead and the restart reports `consumed=0` — a
graceful stop finishes its batch and commits, so there is nothing to replay.

### Stop the database mid-consume

```bash
.venv/bin/heartbeat consume --group dbfail &
sleep 8
docker compose stop postgres
```

```
INFO   partitions revoked: [0, 1, 2]
ERROR  cannot reach PostgreSQL: terminating connection due to administrator command
ERROR  is the stack running? start it with: docker compose up -d
exit code 3
```

It leaves the consumer group cleanly rather than dying while holding partitions.
Restart PostgreSQL and re-run with `--drain`: `duplicates=0`, lag back to zero.

### Take out a broker

```bash
docker compose stop kafka2
.venv/bin/heartbeat topic-info
```

```
partition 0   leader 3   replicas [1, 2, 3]   isr [1, 3]   UNDER-REPLICATED, missing [2]
```

Ingestion continues, because `min.insync.replicas: 2` is still satisfied by the two
survivors. `docker compose start kafka2` and the ISR heals.

### Take out every broker

```bash
docker compose stop kafka1 kafka2 kafka3
```

A running **consumer survives** — its session times out, it rejoins when the brokers
return, and catches up on its own. A **producer** cannot: it reports
`delivered=0 unflushed=200` and exits 1. It never claims success.

### Send a malformed message

```bash
.venv/bin/python - <<'PY'
from confluent_kafka import Producer
p = Producer({"bootstrap.servers": "localhost:9092,localhost:9094,localhost:9096"})
p.produce("heartbeat.raw", key=b"demo", value=b"not json at all")
p.flush(10)
PY
.venv/bin/heartbeat consume --drain --group poison
```

It lands in `heartbeat_rejects` as `not valid JSON`, is copied to `heartbeat.dlq` for
replay, and the consumer carries on.

---

## 8. Change the contract

Add a field and bump the version — it is absorbed:

```bash
.venv/bin/python - <<'PY'
import json, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
p = Producer({"bootstrap.servers": "localhost:9092,localhost:9094,localhost:9096"})
p.produce("heartbeat.raw", key=b"v2", value=json.dumps({
    "schema_version": 2, "event_id": str(uuid.uuid4()), "customer_id": "demo-v2",
    "event_time": datetime.now(timezone.utc).isoformat(), "heart_rate": 72,
    "device_id": "dev-99", "firmware": "2.1"}).encode())
p.flush(10)
PY
.venv/bin/heartbeat consume --drain --group evo
```

The row stores normally — unknown fields are ignored, so an old consumer keeps
working against a new producer.

Remove a required field, change a type or rename one, and it is rejected by name.
Change what a field *means* and it is **stored, silently wrong** — the known gap,
explained in the README.

---

## 9. How hard can it be pushed

The lab runs at 200 readings/second. The consumer sustains about **9,750/second**.

```bash
.venv/bin/heartbeat produce --count 100000 --rate 20000    # build a backlog
time .venv/bin/heartbeat consume --drain --group bench
```

The limit is the offset commit — one network round trip per batch — which is also
what makes a crash replay rather than lose. `consumer.batch_size` in
`config/config.yaml` is the dial between throughput and how much replays.
Numbers in `docs/performance_metrics.md`.

---

## 10. Stop

```bash
docker compose down            # keep the data
docker compose down -v         # delete it
```

---

## Troubleshooting

**A command prints a Python traceback about imports.** Some shells export a
`PYTHONPATH` that leaks other packages in. Prefix the command with `PYTHONPATH=`.

**`cannot reach Kafka` / `cannot reach PostgreSQL`.** The stack is not up. Run
`docker compose up -d` and wait for all five containers to report healthy.

**`create-topics` refuses with a mismatch.** A topic exists but does not match
`config/config.yaml` — usually left over from an older run. Delete it and re-create:

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --delete --topic heartbeat.dlq
```

**Upgrading a stack from before the three-broker change.** The old container and
volume are orphans and the old one holds port 9092:

```bash
docker compose down -v --remove-orphans
docker volume rm heartbeat_kafka_data
```

**The integration tests error rather than skip.** `heartbeat_test` does not exist —
the demo script's reset drops it. Recreate it as shown in section 2.

---

## Where to read next

| Document | Covers |
|---|---|
| [README.md](README.md) | design decisions and why each was made |
| [docs/test_cases.md](docs/test_cases.md) | all 67 test cases with results |
| [docs/performance_metrics.md](docs/performance_metrics.md) | latency, throughput, indexes |
| [docs/architecture.svg](docs/architecture.svg) | data flow diagram |
| [docs/sample_output/](docs/sample_output/) | logs from a real proof run |
