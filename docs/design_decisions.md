# Design decisions

Why this is built the way it is. Each section is a choice that could reasonably have
gone another way, and the reason it did not.

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

### The classifications are ranges, not diagnoses

`bradycardia`, `tachycardia` and `critical` name fixed bands — below 60, above 100,
above 180 — applied to a single reading. They are not clinical findings, and the
system does not diagnose.

Nothing here accounts for **age**, and maximum heart rate is roughly 220 minus it, so
185 bpm is unremarkable in someone of 25 exercising and an emergency in someone of 80
at rest. Nor for **activity**, **medication**, or a patient's **own baseline** — a
trained athlete resting at 48 and a patient on beta blockers at 55 are both normal
and both land in `bradycardia` here. The thresholds are resting adult values, and
`critical` is this project's label rather than a clinical term.

That is a limitation of the data available, not an oversight: the message contract
carries `customer_id`, `event_time` and `heart_rate`, and nothing else to reason
with. A system making clinical claims would need each patient's own baseline, which
is the same conclusion the dashboard reaches when it declines to rank bradycardia
and the alert reaches when it detects a fleet-level surge rather than an individual
in distress.

### What a change to the message contract costs

Each kind of change was pushed through the running pipeline to see what it actually
does, rather than reasoned about:

| Change | Result |
|---|---|
| Add a field, bump `schema_version` | **stored** — unknown keys are ignored, so old consumers keep working |
| Remove a required field | rejected: `missing field(s): heart_rate` |
| Change a type | rejected: `heart_rate must be an integer, got '72'` |
| Rename a field | rejected: `missing field(s): heart_rate` |
| Change what a field *means* | **stored, silently wrong** |

Structural breakage is loud. Every one lands in `heartbeat_rejects` naming itself,
carrying the partition and offset it came from, and the pipeline keeps running.

The last row is the dangerous one, and it is not hypothetical — units and timezones
are among the most common real data faults. A firmware update reporting a different
unit, or `event_time` arriving in local time, produces structurally perfect JSON.
It parses, classifies and stores. Nothing rejects it and nothing alerts. Every
affected reading is wrong and this system will not tell you.

### `schema_version` is carried, and deliberately not enforced

The contract includes `schema_version`. The consumer reads it and does not gate on
it, because rejecting an unknown version would turn a backward-compatible change
into a total outage — the opposite of what a version field should buy.

It is also **not stored**. A v2 message with extra fields lands in
`heartbeat_readings` indistinguishable from a v1 one: same columns, nothing to
separate them. That is a deliberate trade resting on four assumptions, all true here:

1. **One producer implementation.** Nothing else writes to this topic.
2. **Semantic changes arrive with structural ones**, so validation catches them —
   the assumption the table above shows to be the weakest.
3. **Both ends ship together.** Producer and consumer are one repository and one
   deploy, so there is no window where a v2 producer runs against a v1 consumer.
4. **Quarantine can be time-based.** Every row records `ingested_at`,
   `kafka_partition` and `kafka_offset`, so a bad producer's output can be found by
   time window or offset range. Coarser than filtering on a version, but sufficient.

If any of those stops holding — a second team producing, or independent deploys —
the fix is one column: add `schema_version` to `heartbeat_readings` and to the
insert. It is deliberately not there yet, because a column nobody queries is not
free to a reader trying to understand the schema.

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

### Mounting a volume is not the same as using it

The brokers mount `kafka1_data:/var/lib/kafka/data`, which is the path the Confluent
image uses. This is the Apache image, and it defaults `log.dirs` to
`/tmp/kraft-broker-logs` — inside the container's own writable layer. The volume was
mounted and empty while every message went somewhere `docker compose down` throws
away, so a restart came back with no topics at all and the producer failed with
`UNKNOWN_TOPIC_OR_PART`.

`KAFKA_LOG_DIRS` now points at the mounted path. Note that
`/opt/kafka/config/broker.properties` still reads `/tmp/kraft-broker-logs` inside
the container — the entrypoint applies the environment without rewriting that file,
so the config on disk is not evidence of where the data goes. The volume contents
are: `__cluster_metadata-0`, `heartbeat.raw-0`, `heartbeat.dlq-0`.

Verified by cycling the stack: 300 messages on `heartbeat.raw` before
`docker compose down`, 300 after `up`, replication intact.

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

### The dashboard answers two questions, and says which is which

It is laid out as four bands, because a monitoring screen is read top to bottom
under time pressure:

1. **Fleet** — patients monitored, patients needing attention, ingest latency,
   rejections. `Patients needing attention` counts *people whose latest reading is
   abnormal*, not readings, so one noisy sensor cannot inflate it.
2. **Selected patient** — their actual readings in order, plus highest, lowest,
   average, count and abnormal count for the window. This sits directly under the
   fleet summary because it is what someone opens the dashboard to look at.
3. **Who needs attention** — patients ranked by how many critical and tachycardia
   episodes they logged across the window, with their peak and lowest reading.
4. **Pipeline health** — classification mix and every rejection with its offset.

The patient selector is single-select on purpose. Earlier it defaulted to *All* and
filtered exactly one of eight panels, so selecting a patient changed almost nothing
on screen while appearing to. Now every panel is either scoped to the selected
patient and titled with their id, or explicitly labelled as fleet-wide.

### A tile and an alert that disagree

After calibrating the alert I left the `Critical readings` tile counting over the
dashboard's time range with red at five or more. It sat permanently red at 224 while
the alert, looking at the same system, reported everything normal at 43 a minute.
One of them had to be wrong and it was the tile.

A count over the picker's range cannot carry a threshold at all: switch from five
minutes to fifteen and it triples with nothing having changed. The tile is now a
rate over the last minute using the alert's own calibration — amber above two
standard deviations, red at 120 where the alert fires — so the two cannot contradict
each other again.

The wider point is that calibrating one indicator and not its neighbour leaves the
dashboard less trustworthy than before, because now they disagree in front of you.

### An alert calibrated against nothing fires against everything

The alert first asked whether any patient had a critical reading in the last five
minutes, with a threshold of *greater than zero*. The answer was 45 of 50 patients,
so it fired permanently. That is the same mistake as counting patients on the fleet
tile — anomalies here are spread evenly, so any presence test is always true — and a
permanently firing alert is not an alert, it is background noise nobody reads.

It is now calibrated against the measured baseline. Twenty minutes of normal traffic
runs at 36.6 critical readings a minute, standard deviation 14.9, peak 54. The
threshold is 120 a minute sustained for two minutes — roughly five standard
deviations out — so ordinary variation is silent.

Both halves were verified rather than assumed. Under normal load it stayed inactive
for six minutes with the rate moving between 39 and 57. Sustaining an injected surge
walked it `inactive → pending → firing` at about 490 a minute.

It is a fleet-level surge detector, not a clinical alarm. Alerting on an individual
patient needs a baseline for that patient, for the same reason the dashboard stopped
ranking bradycardia: a fixed threshold cannot tell someone who is deteriorating from
someone who simply runs low.

### Absence of data is the failure nothing else catches

The first version of this tile counted anyone seen anywhere in the selected time
range. Kill the producer and it kept reading **50** for a whole window, because those
patients were still inside it, while the newest reading aged and every panel carried
on showing the last good data. A dead sensor looked exactly like a healthy patient.

Adding a second tile that detected the silence was not enough on its own: the two
then read `50` and `50` side by side, and a reader had to know that *monitored*
secretly meant *appeared at some point* rather than *is being monitored*.

So the tile itself changed. `Patients reporting` counts patients that have sent a
reading in the last sixty seconds, and turns red when it falls. Paired with
`Patients gone silent` the two now move together:

```
t+60s   reporting=50   silent=0     still inside the liveness window
t+80s   reporting=0    silent=50    the flip
```

One story instead of two numbers that have to be reconciled.

A count alone is still not actionable, though — it tells you three patients have
gone quiet without telling you which three, and a silent patient appears in no other
panel because every one of them is built from readings that patient is not sending.
So `Patients gone silent — longest first` lists them with how long each has been
quiet, sorted by the one you have heard from least recently. It is empty whenever
the fleet is healthy.

For a monitor this is the failure that matters most, because it is the one that
looks like good news.

### Ranking on one reading is not a patient's condition

The attention list first showed each patient's *latest* reading. Sampled three
times over eight seconds it returned 15, then 12, then 8 patients, and the set of
critical ones changed completely each time. Patients appeared to enter and leave
crisis every refresh, because a single reading is a coin flip, not a condition.

It now counts episodes across the whole window. The same patients hold the top,
their counts drifting by one as the window slides — which is a sliding window
behaving correctly rather than noise.

Two measurement decisions came out of looking at the data rather than assuming:

**Bradycardia is not ranked.** It tracks resting baseline. A patient who normally
sits at 60 bpm logged 253 crossings in five minutes while one at 78 logged two —
that is a characteristic, not deterioration. A fixed threshold cannot tell them
apart; a real system would alert on change from each patient's own baseline, which
needs per-patient baselines this project does not build.

**The fleet tile counts readings, not patients.** All 50 patients logged a critical
or tachycardia reading in five minutes, so a patient count reads 50 of 50 and says
nothing. That is an artefact of the generator spreading anomalies evenly; real
distress concentrates in a few people, and there the patient count would be the
right measure. The tile says which it is counting.

### The patient chart plots readings, not an aggregate

One patient produces about one reading a second, so fifteen minutes is under a
thousand points — few enough to draw every one. The panel does exactly that, and
nothing is aggregated away.

It began as an average per interval, which was wrong in a way worth keeping in the
record. Readings arrive faster than a chart has pixels, so each point covered an
interval holding dozens of them. Averaged, one minute of this data plots **73 bpm**
while the readings inside span **20 to 250** — a patient at 250 drawn as normal, on
the same screen as a table listing them critical.

Switching to the maximum would have fixed that and broken the opposite case: a
bucket of 31, 72, 75, 80 plots 80, and the patient at 31 disappears. This system
alerts on both ends, so neither aggregate is safe — which is why the patient panel
plots the readings themselves.

The summary beside it shows highest, lowest and average together for the same
reason: for the patient above, the average is 79 and the highest is 250. Either
number alone misleads.

The general point: an aggregate chosen for a dashboard is a claim about which
information you are willing to lose. For vitals, the average is the one value you
can afford least.

### Grafana is provisioned from files

The datasource, dashboard and alert rule are YAML and JSON in `docker/grafana/`,
mounted into the container. Configuring Grafana through its UI would put that state
in a Docker volume, and a fresh clone would come up with an empty Grafana.

### Deliberately not included

Schema Registry and Avro (a shared schema module and strict parsing are proportionate
here), Prometheus and a Kafka metrics exporter (consumer lag is one CLI command), and
Spark (the same topic could feed a windowed aggregation, but the per-record path does
not need it). Reaching for those at this scale would be over-engineering.

