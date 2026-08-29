# Test plan

Three layers. Unit tests cover the pure logic with no infrastructure; integration
tests cover the guarantees that live in the DDL rather than in Python; the demo
script covers what only appears with a real broker and a real crash.

```bash
.venv/bin/python -m pytest            # 61 unit tests
./scripts/demo_failure_modes.sh       # end-to-end proofs (resets the stack)
```

The 11 integration tests are skipped unless pointed at a disposable database, since
they truncate their tables.

CI runs all three layers on every push: the unit tests, the integration tests against
a PostgreSQL service, and then the demo script against the repository's own
`docker-compose.yml`. The end-to-end results below are therefore re-proven per commit,
not recorded once — each run attaches its logs as a build artifact.

```bash
HEARTBEAT_TEST_DSN="host=localhost port=5434 dbname=heartbeat_test user=heartbeat password=..." \
HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS=1 .venv/bin/python -m pytest
```

The demo script's reset (`docker compose down -v`) drops this database along with
everything else, so recreate it when you have just run the proofs:

```bash
docker compose exec postgres psql -U heartbeat -d postgres -c "CREATE DATABASE heartbeat_test;"
```

## Unit tests

| # | Case | Expected | Result |
|---|---|---|---|
| U1 | Event survives a JSON round trip | Parsed event equals the original | Pass |
| U2 | Two new events with the same inputs | Different `event_id` | Pass |
| U3 | Message key | Equals `customer_id` as bytes | Pass |
| U4 | Payload is not JSON | `SchemaError: not valid JSON` | Pass |
| U5 | Payload is a JSON array | `SchemaError: expected a JSON object` | Pass |
| U6 | Payload is not valid UTF-8 | `SchemaError`, no crash | Pass |
| U7 | Required fields missing | Error names every missing field | Pass |
| U8 | `heart_rate` is `true` | Rejected — `bool` subclasses `int` | Pass |
| U9 | `heart_rate` is a string, float or null | Rejected | Pass |
| U10 | `event_time` has no timezone offset | Rejected | Pass |
| U11 | `event_time` is unparseable | Rejected | Pass |
| U12 | `event_id` is not a UUID | Rejected | Pass |
| U13 | `event_id` or `customer_id` is empty | Rejected | Pass |
| U14 | Unknown `schema_version` with an extra field | Accepted, version carried | Pass |
| U15 | Band boundaries 20, 59, 60, 100, 101, 180, 181, 250 | brady, brady, normal, normal, tachy, tachy, critical, critical | Pass |
| U16 | Values -1, 0, 19, 251, 999 | `ImplausibleReading` | Pass |
| U17 | 190 bpm | `critical` — stored, not discarded | Pass |
| U18 | Class names | Match the database `CHECK` constraint exactly | Pass |
| U19 | Heart rate bands out of ascending order | `ConfigError` | Pass |
| U20 | Generator rates outside 0–1, or summing above 1 | `ConfigError` | Pass |
| U21 | Zero customers, zero rate, zero batch size or timeout | `ConfigError` | Pass |

U14 is the schema evolution case: adding an optional field to the contract must not
break a consumer that has not been updated.

U18 guards a real coupling — `HeartRateClass` and the `hr_class_known` constraint in
`sql/001_schema.sql` are two independent lists of the same four values, and a new
class added to one and not the other would fail at insert time in production rather
than in the test suite.

## Integration tests

Against a real PostgreSQL. These exist because the deduplication guarantee is
enforced by the schema, not by application code, and a mock would happily agree
with whatever the code claims.

| # | Case | Expected | Result |
|---|---|---|---|
| I1 | Schema applied to an empty database | Both tables present and empty | Pass |
| I2 | A batch of readings written | Row count matches the batch | Pass |
| I3 | The same batch written again | 0 inserted, total unchanged | Pass |
| I4 | A batch overlapping a previous one | Only the new rows inserted | Pass |
| I5 | The same reject written twice | Second is suppressed by `(partition, offset)` | Pass |
| I6 | Reject payload is not valid UTF-8 | Stored — the column is `bytea` for this reason | Pass |
| I7 | Heart rate of 900 | `CheckViolation` from the database | Pass |
| I8 | Classification not in the constraint list | `CheckViolation` from the database | Pass |
| I9 | A batch where one row violates a constraint | Whole batch rolls back, nothing stored | Pass |
| I10 | Writing again after a failed batch | Succeeds — the connection is still usable | Pass |
| I11 | `ingested_at` | Stamped by the database, at or after `event_time` | Pass |

I9 is the one that justifies the transaction. Without it a batch could land half
written while the committed offset claimed all of it had been stored — the exact
gap that makes at-least-once delivery unsafe.

I7 and I8 are defence in depth. The consumer already rejects those values before
they reach the database; these assert that a future consumer which skipped
validation would still be stopped.

## End-to-end tests

| # | Case | Method | Expected | Result |
|---|---|---|---|---|
| E1 | Topics are created | `heartbeat create-topics` | `heartbeat.raw` with 3 partitions, `heartbeat.dlq` with 1 | Pass |
| E2 | Creation is idempotent | Run it twice | Second run reports "already exists and matches config", no error | Pass |
| E3 | Schema on a fresh volume | `docker compose up -d` | 2 tables, 7 indexes, no manual step | Pass |
| E4 | Schema is idempotent | `heartbeat init-db` on an existing database | Applies cleanly, no error | Pass |
| E5 | Readings reach Kafka | `heartbeat produce --count 500` | `sent=500 delivered=500 failed=0` | Pass |
| E6 | Keying holds ordering | Consume 500 and group by key | No customer appears on more than one partition | Pass |
| E7 | Readings reach PostgreSQL | `heartbeat consume --drain` | 492 stored, 8 rejected, 0 duplicates | Pass |
| E8 | Abnormal readings are kept | Query by `hr_class` | 14 tachycardia/critical rows stored, not dropped | Pass |
| E9 | Replay does not duplicate | Re-consume the same topic as a new group | `inserted=0 duplicates=492`, row counts unchanged | Pass |
| E10 | Consumer killed mid-stream | `SIGKILL` with the producer still running, then restart | Stored + rejected equals produced; no duplicate `event_id` | Pass |
| E11 | Second consumer joins | Start a second member of the group | Partitions redistribute; both logs show the reassignment | Pass |
| E12 | Malformed messages | Inject 5 broken payloads | All 5 in `heartbeat_rejects` with distinct reasons; consumer keeps running | Pass |
| E13 | Poison forwarded for replay | Check `heartbeat.dlq` offsets | Rejected messages present on the topic | Pass |
| E14 | Consumer lag is observable | `kafka-consumer-groups.sh --describe` | Per-partition lag reported | Pass |
| E15 | Grafana is provisioned | `docker compose up -d` on a clean volume | Datasource connects, dashboard and alert rule present | Pass |
| E16 | Alert fires on a real condition | Ingest readings above the critical threshold | Rule goes `pending` then `firing` with value 1 | Pass |
| E17 | Topics are replicated | `heartbeat topic-info` | Every partition `replicas [1,2,3] isr [1,2,3]` | Pass |
| E18 | A topic that exists but does not match config | Recreate the DLQ at RF=1, run `create-topics` | Refused with the mismatch named, exit 1 | Pass |
| E19 | A broker dies mid-stream | `docker compose stop kafka2` while producing | Ingestion continues (7832 → 10574 rows) | Pass |
| E20 | Leadership fails over | Compare `topic-info` before and during | Partition 2 leader moves from broker 2 to 3 | Pass |
| E21 | In-sync replicas shrink | `topic-info` while one broker is down | All 4 partitions under-replicated, missing [2] | Pass |
| E22 | The broker rejoins | `docker compose start kafka2` | ISR returns to [1,2,3] on every partition | Pass |
| E23 | No duplicates across a failover | Count after the whole episode | 19,623 rows, 19,623 distinct `event_id` | Pass |

E19 is the claim; E20 and E21 are why it held. Without them, "ingestion continued"
could just mean nothing was actually broken.

E18 exists because of a defect this caught — see below.

E10 is the one that matters. It asserts two things separately — that
`stored + rejected` equals what was produced (nothing lost) and that
`count(DISTINCT event_id)` equals `count(*)` (nothing duplicated). Either could fail
alone, and each has a different cause.

## A defect this plan found

E10 failed on its first honest run: `expected 2000, got 664`. A consumer killed with
`SIGKILL` does not leave its consumer group, so the broker holds its partitions until
`session.timeout.ms` expires. The replacement consumer polled empty for that whole
window, and `--drain` treated "no partitions assigned yet" as "topic is caught up"
and exited, leaving 1336 records unprocessed while reporting success.

Fixed by having drain wait until it actually holds an assignment before treating
empty polls as a drained topic, and by lowering `session.timeout.ms` from 45 s to
10 s so a dead member is detected quickly.

The earlier version of this test could not have caught it: it killed the consumer
after the backlog had already been drained, so the recovery pass had nothing to do
and passed for the wrong reason.

## A second defect, found by adding replication

The first three-broker run reported
`PASS ingestion continued through the broker failure` — the
important assertion, green — while `heartbeat.dlq` was quietly running at
replication factor 1.

Two causes. Kafka's `auto.create.topics.enable` defaults to on, so something touching
the topic before `create-topics` ran had already created it at the broker defaults:
one partition, no replication. And `create-topics` only checked whether a topic
*existed*, not whether it matched the spec — so it reported "already exists" and moved
on while config claimed RF=3.

Fixed by disabling auto-creation, and by having `create-topics` compare partition
count and replication factor against config and refuse to continue on a mismatch
(E18). A green assertion on a system that is not doing what its configuration says is
worse than a failing one.

## Not covered

**Losing the host.** The three brokers are containers on one machine, so E19–E23
prove a broker *process* can die, not that the hardware under all three can. Real
clusters spread brokers across failure domains; nothing here tests that.

**Losing two brokers at once.** With `min.insync.replicas: 2`, writes stop — by
design, since one surviving copy is not the guarantee the config promises. That is
correct behaviour rather than a failure, but it is asserted nowhere.

**TLS and authentication.** Every listener is plaintext. Out of scope for this lab.

**Grafana alert delivery.** E16 proves the rule evaluates and fires. Whether a
notification reaches anyone is untested — no contact point is configured.
