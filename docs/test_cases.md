# Test plan

Two layers. Unit tests cover the pure logic and run in under a second with no
infrastructure; the demo script covers the behaviour that only appears when a real
broker, a real database and a real crash are involved.

```bash
.venv/bin/python -m pytest            # 43 unit tests
./scripts/demo_failure_modes.sh       # end-to-end proofs (resets the stack)
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

## End-to-end tests

| # | Case | Method | Expected | Result |
|---|---|---|---|---|
| E1 | Topics are created | `heartbeat create-topics` | `heartbeat.raw` with 3 partitions, `heartbeat.dlq` with 1 | Pass |
| E2 | Creation is idempotent | Run it twice | Second run reports "already exists", no error | Pass |
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

## Not covered

Broker failure and recovery, since there is one broker and replication factor 1 —
losing it loses data by construction, which is a known property rather than a
testable behaviour here. Multi-broker failover, TLS and authentication are out of
scope for this lab.
