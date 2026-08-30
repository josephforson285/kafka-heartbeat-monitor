# Failure modes

What breaks it, what survives, and what is lost. Every figure here came from running
the thing, not from reasoning about it.

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

PASS  ingestion continued through the broker failure (7822 -> 10254 rows)
PASS  in-sync replicas degraded while it was down (4)
PASS  in-sync replicas recovered (0)
PASS  still no duplicates (19623)
```

Leadership moved off the dead broker on its own and the pipeline kept writing —
`min.insync.replicas: 2` was still satisfied by the two survivors. Note that
leadership stays on broker 3 after recovery; Kafka does not move it back
automatically.

### What survives a sudden shutdown

The committed offset is the boundary. Everything up to it is durable in PostgreSQL;
everything after it is replayed and absorbed by the primary key. Each component was
killed on purpose to check:

| What dies | What is lost | What happens on resume |
|---|---|---|
| Consumer, `SIGTERM` | nothing — the batch in hand finishes and commits | resumes at the committed offset, replays nothing |
| Consumer, `SIGKILL` | nothing durable — the uncommitted batch is still on the topic | replays that batch, `ON CONFLICT` absorbs it |
| PostgreSQL | nothing durable — same uncommitted batch | leaves the group cleanly, exits 3; replays on restart |
| Every broker, consumer running | nothing — with no messages there is nothing to commit | self-heals: session times out, rejoins, carries on |
| Every broker, producer running | every buffered reading | reported as `unflushed`, exit 1. Those readings are gone |
| Producer, `SIGTERM` | nothing — `close()` flushes what is buffered | starts fresh; there is nothing to replay |
| Producer, `SIGKILL` | whatever sat in the local buffer, **with no record of it** | starts fresh; the gap is permanent |

The consumer side is safe because the order is fixed: write the rows, commit the
offsets. Interrupt it anywhere and the worst case is that a batch is processed twice,
which the primary key makes free.

**The producer is the exception, and it is worth being straight about.** `produce()`
buffers locally and delivery is confirmed later on a callback, so a `SIGKILL` takes
the buffer with it — measured at roughly 51 readings out of 1,200 at 300/s, and the
process dies before it can report a single one. There is no replay, because a heart
rate sensor has no log to replay from. `acks=all` and `enable.idempotence` protect a
message once Kafka has been asked to store it; they cannot protect one that never
left the producer.

A real deployment closes that gap on the device: the sensor buffers to its own
storage and retries. Nothing on the Kafka side can do it for you.

The figures above are from the run whose logs are committed in
[sample_output/](sample_output/); re-running the script regenerates both
together. Screenshots of the running system are in
[screenshots/](screenshots/), kept separate because the demo script
overwrites everything in `sample_output/`. Alongside the logs is
[database-contents.txt](sample_output/database-contents.txt) — the table
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

## Evidence

Rejections carry the reason and the exact log position they came from — nothing is
silently dropped:

![rejections](screenshots/07-rejects.png)

The dashboard's pipeline health band shows the same, live:

![pipeline health](screenshots/02-dashboard-pipeline-health.png)
