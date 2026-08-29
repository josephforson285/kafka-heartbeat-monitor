#!/usr/bin/env bash
#
# Failure-mode proofs for the heartbeat pipeline:
#
#   1. consumer killed mid-batch -> nothing lost, nothing duplicated
#   2. a second consumer joins   -> partitions redistribute
#   3. malformed messages arrive -> recorded and skipped, pipeline survives
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

mkdir -p "$OUT"
psql_q() { docker compose exec -T postgres psql -U "$POSTGRES_USER" -d heartbeat -tAq -c "$1"; }
banner() { printf '\n══ %s\n' "$*"; }
check()  { if [ "$2" = "$3" ]; then printf 'PASS  %s (%s)\n' "$1" "$2"; else printf 'FAIL  %s: expected %s, got %s\n' "$1" "$3" "$2"; exit 1; fi; }


banner "reset"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null 2>&1
# bounded, so an unhealthy container fails the run instead of hanging it
for _ in $(seq 90); do
  [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -eq 3 ] && break
  sleep 2
done
if [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -ne 3 ]; then
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
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group proof2 \
  | awk '{printf "  %s\n", $0}' | tee "$OUT/proof2-lag.txt"

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
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic heartbeat.dlq | sed 's/^/  /'

banner "database contents"
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d heartbeat \
  -c "\d heartbeat_readings" \
  -c "SELECT hr_class, count(*), min(heart_rate), max(heart_rate) FROM heartbeat_readings GROUP BY 1 ORDER BY 2 DESC;" \
  -c "SELECT customer_id, event_time, heart_rate, hr_class, kafka_partition, kafka_offset FROM heartbeat_readings ORDER BY event_time DESC LIMIT 10;" \
  -c "SELECT reason, count(*) FROM heartbeat_rejects GROUP BY 1 ORDER BY 2 DESC;" \
  -c "SELECT count(*) AS total_readings, count(DISTINCT event_id) AS distinct_event_ids FROM heartbeat_readings;" \
  | tee "$OUT/database-contents.txt"

banner "all proofs passed"
