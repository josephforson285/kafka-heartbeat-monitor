#!/usr/bin/env bash
#
# Failure-mode proofs for the heartbeat pipeline:
#
#   1. consumer killed mid-batch -> nothing lost, nothing duplicated
#   2. a second consumer joins   -> partitions redistribute
#   3. malformed messages arrive -> recorded and skipped, pipeline survives
#   4. a broker dies             -> leadership moves, ingestion continues
#
# Destructive: tears the stack down with its volumes and starts clean.
# Logs land in docs/sample_output/.

set -euo pipefail
cd "$(dirname "$0")/.."

set -a; . ./.env; set +a
# the repo venv is the only interpreter that should be on the path here
export PYTHONPATH=
HB=.venv/bin/heartbeat
OUT=docs/sample_output
EVENTS=2000
RATE=200
raw_partitions=3

mkdir -p "$OUT"
psql_q() { docker compose exec -T postgres psql -U "$POSTGRES_USER" -d heartbeat -tAq -c "$1"; }
# one place to name a broker container, so renaming a service cannot leave a
# stale reference sitting in the middle of a pipeline
kafka_cli() { local tool="$1"; shift; docker compose exec -T kafka1 "/opt/kafka/bin/$tool" "$@"; }
banner() { printf '\n══ %s\n' "$*"; }
check()  { if [ "$2" = "$3" ]; then printf 'PASS  %s (%s)\n' "$1" "$2"; else printf 'FAIL  %s: expected %s, got %s\n' "$1" "$3" "$2"; exit 1; fi; }


banner "reset"
# --remove-orphans matters when upgrading from the single-broker layout: the old
# `kafka` service is no longer in this file, so a plain `down` leaves it running
# and holding port 9092
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d >/dev/null 2>&1
# bounded, so an unhealthy container fails the run instead of hanging it
for _ in $(seq 90); do
  [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -eq 5 ] && break
  sleep 2
done
if [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -ne 5 ]; then
  printf 'stack did not become healthy:\n'
  docker compose ps
  exit 1
fi
$HB create-topics


banner "proof 1 — consumer killed mid-batch"
# both run at once, so the kill lands on a consumer that still has work queued
$HB produce --count "$EVENTS" --rate "$RATE" >/dev/null 2>&1 &
producer=$!
$HB consume --group proof1 2>"$OUT/proof1-crash.log" &
victim=$!

sleep 4
kill -9 "$victim"
wait "$victim" 2>/dev/null || true
partial=$(psql_q "SELECT count(*) FROM heartbeat_readings")
printf 'killed with SIGKILL after %s of %s rows were written\n' "$partial" "$EVENTS"

wait "$producer" 2>/dev/null || true
$HB consume --drain --group proof1 2>"$OUT/proof1-recovery.log"
tail -1 "$OUT/proof1-recovery.log"

stored=$(psql_q "SELECT count(*) FROM heartbeat_readings")
rejected=$(psql_q "SELECT count(*) FROM heartbeat_rejects")
distinct=$(psql_q "SELECT count(DISTINCT event_id) FROM heartbeat_readings")

check "no records lost"       "$((stored + rejected))" "$EVENTS"
check "no records duplicated" "$distinct"              "$stored"


banner "proof 2 — a second consumer joins the group"
# latency is only meaningful for rows written while a consumer was actually live
window_start=$(psql_q "SELECT now()")
$HB produce --duration 20 --rate "$RATE" >/dev/null 2>&1 &
producer=$!

$HB consume --group proof2 2>"$OUT/proof2-consumer-a.log" &
first=$!
sleep 8
$HB consume --group proof2 2>"$OUT/proof2-consumer-b.log" &
second=$!
sleep 12

printf '\nconsumer A partition history:\n'
grep -E 'partitions (assigned|revoked)' "$OUT/proof2-consumer-a.log" | sed 's/^/  /'
printf 'consumer B partition history:\n'
grep -E 'partitions (assigned|revoked)' "$OUT/proof2-consumer-b.log" | sed 's/^/  /'

printf '\nconsumer group lag while both are running:\n'
kafka_cli kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group proof2 | awk '{printf "  %s\n", $0}' | tee "$OUT/proof2-lag.txt"

kill -TERM "$first" "$second" 2>/dev/null || true
wait "$first" "$second" "$producer" 2>/dev/null || true

printf '\nend-to-end latency for rows written during this window:\n'
psql_q "SELECT '  rows=' || count(*)
        || '  p50=' || round(percentile_cont(0.5) WITHIN GROUP (
               ORDER BY extract(epoch FROM ingested_at - event_time)) * 1000) || 'ms'
        || '  p95=' || round(percentile_cont(0.95) WITHIN GROUP (
               ORDER BY extract(epoch FROM ingested_at - event_time)) * 1000) || 'ms'
        || '  max=' || round(max(extract(epoch FROM ingested_at - event_time)) * 1000) || 'ms'
        FROM heartbeat_readings WHERE ingested_at > '$window_start'" \
  | tee "$OUT/proof2-latency.txt"


banner "proof 3 — malformed messages"
# count only contract violations; the generator is emitting implausible values of
# its own the whole time, and those are a different kind of reject
schema_rejects="SELECT count(*) FROM heartbeat_rejects WHERE reason NOT LIKE '%outside plausible range%'"
before=$(psql_q "$schema_rejects")

.venv/bin/python - <<'PY'
from confluent_kafka import Producer

poison = [
    b"not json at all",
    b'{"customer_id": "cust-0001"}',
    b'{"event_id":"not-a-uuid","customer_id":"c","event_time":"2026-01-01T00:00:00+00:00","heart_rate":70}',
    b'{"event_id":"3f1b6d84-0f1a-4a1e-9a5e-1c2d3e4f5a6b","customer_id":"c","event_time":"2026-01-01T00:00:00+00:00","heart_rate":"seventy"}',
    b"\xff\xfe not valid utf-8",
]
producer = Producer({"bootstrap.servers": "localhost:9092"})
for payload in poison:
    producer.produce("heartbeat.raw", key=b"cust-0001", value=payload)
producer.flush(10)
print(f"injected {len(poison)} malformed messages")
PY

$HB consume --drain --group proof2 2>"$OUT/proof3-poison.log"
tail -1 "$OUT/proof3-poison.log"

after=$(psql_q "$schema_rejects")
check "every malformed message recorded" "$((after - before))" "5"

printf '\nwhy each was rejected:\n'
psql_q "SELECT '  ' || reason FROM heartbeat_rejects
        WHERE reason NOT LIKE '%outside plausible range%'
        ORDER BY reject_id DESC LIMIT 5"

printf '\nmessages forwarded to the dead-letter topic:\n'
kafka_cli kafka-get-offsets.sh --bootstrap-server localhost:9092 \
  --topic heartbeat.dlq | sed 's/^/  /'

banner "proof 4 — a broker fails"
printf 'replication before:\n'
$HB topic-info | sed 's/^/  /' | tee "$OUT/proof4-before.txt"

# every partition is replicated on all three brokers, so losing one degrades all of them
partitions=$(( raw_partitions + 1 ))

$HB produce --duration 70 --rate "$RATE" >/dev/null 2>&1 &
producer=$!
$HB consume --group proof4 2>"$OUT/proof4-consumer.log" &
consumer=$!
sleep 10
before_rows=$(psql_q "SELECT count(*) FROM heartbeat_readings")

docker compose stop kafka2 >/dev/null 2>&1
printf '\nstopped kafka2 while data was flowing\n'
# poll rather than sleep: how fast the controller shrinks the ISR is not ours to assume
for _ in $(seq 40); do
  [ "$($HB topic-info 2>/dev/null | grep -c UNDER-REPLICATED || true)" -eq "$partitions" ] && break
  sleep 2
done
printf 'replication while one broker is down:\n'
$HB topic-info | sed 's/^/  /' | tee "$OUT/proof4-degraded.txt"
sleep 8
during_rows=$(psql_q "SELECT count(*) FROM heartbeat_readings")
printf 'rows stored: %s before the failure, %s while degraded\n' "$before_rows" "$during_rows"

docker compose start kafka2 >/dev/null 2>&1
printf '\nrestarted kafka2, waiting for it to rejoin the in-sync replicas\n'
for _ in $(seq 60); do
  $HB topic-info 2>/dev/null | grep -q UNDER-REPLICATED || break
  sleep 2
done
printf 'replication after recovery:\n'
$HB topic-info | sed 's/^/  /' | tee "$OUT/proof4-recovered.txt"

kill -TERM "$consumer" 2>/dev/null || true
wait "$consumer" "$producer" 2>/dev/null || true
$HB consume --drain --group proof4 2>>"$OUT/proof4-consumer.log"

stored=$(psql_q "SELECT count(*) FROM heartbeat_readings")
distinct=$(psql_q "SELECT count(DISTINCT event_id) FROM heartbeat_readings")

if [ "$during_rows" -gt "$before_rows" ]; then
  printf 'PASS  ingestion continued through the broker failure (%s -> %s rows)\n' \
    "$before_rows" "$during_rows"
else
  printf 'FAIL  ingestion stalled during the broker failure\n'; exit 1
fi
check "in-sync replicas degraded while it was down" \
  "$(grep -c UNDER-REPLICATED "$OUT/proof4-degraded.txt" || true)" "$partitions"
check "in-sync replicas recovered" \
  "$(grep -c UNDER-REPLICATED "$OUT/proof4-recovered.txt" || true)" "0"
check "still no duplicates" "$distinct" "$stored"


banner "database contents"
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d heartbeat \
  -c "\d heartbeat_readings" \
  -c "SELECT hr_class, count(*), min(heart_rate), max(heart_rate) FROM heartbeat_readings GROUP BY 1 ORDER BY 2 DESC;" \
  -c "SELECT customer_id, event_time, heart_rate, hr_class, kafka_partition, kafka_offset FROM heartbeat_readings ORDER BY event_time DESC LIMIT 10;" \
  -c "SELECT reason, count(*) FROM heartbeat_rejects GROUP BY 1 ORDER BY 2 DESC;" \
  -c "SELECT count(*) AS total_readings, count(DISTINCT event_id) AS distinct_event_ids FROM heartbeat_readings;" \
  | tee "$OUT/database-contents.txt"

banner "all proofs passed"
