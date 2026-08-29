"""Database-level guarantees, against a real PostgreSQL.

The dedup story lives in the DDL, not in Python: a mock cannot tell you whether
ON CONFLICT actually suppresses a replayed batch, or whether a failed batch rolls
back cleanly. These tests truncate their tables, so they only run when explicitly
opted into against a disposable database.
"""

import os
import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from heartbeat.config import SCHEMA_SQL_PATH
from heartbeat.db import HeartbeatStore

DSN = os.getenv("HEARTBEAT_TEST_DSN")
OPTED_IN = os.getenv("HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS") == "1"

pytestmark = pytest.mark.skipif(
    not (DSN and OPTED_IN),
    reason="needs HEARTBEAT_TEST_DSN and HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS=1",
)


@pytest.fixture
def store():
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA_SQL_PATH.read_text())
        conn.execute("TRUNCATE heartbeat_readings, heartbeat_rejects")
    open_store = HeartbeatStore(DSN)
    yield open_store
    open_store.close()


def reading(offset, *, heart_rate=72, hr_class="normal", event_id=None, partition=0):
    return (
        event_id or str(uuid.uuid4()),
        "cust-0001",
        datetime.now(timezone.utc),
        heart_rate,
        hr_class,
        partition,
        offset,
    )


def count(table: str) -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_schema_applies_to_an_empty_database(store):
    assert count("heartbeat_readings") == 0
    assert count("heartbeat_rejects") == 0


def test_a_batch_is_stored(store):
    assert store.write_batch([reading(0), reading(1)], []) == (2, 0)


def test_replaying_a_batch_inserts_nothing(store):
    batch = [reading(0), reading(1), reading(2)]
    assert store.write_batch(batch, [])[0] == 3
    assert store.write_batch(batch, [])[0] == 0
    assert count("heartbeat_readings") == 3


def test_a_partially_overlapping_replay_inserts_only_the_new_rows(store):
    first = [reading(0), reading(1)]
    store.write_batch(first, [])
    assert store.write_batch([*first, reading(2)], [])[0] == 1
    assert count("heartbeat_readings") == 3


def test_rejects_deduplicate_on_partition_and_offset(store):
    rejects = [(b"garbage", "not valid JSON", 0, 7)]
    assert store.write_batch([], rejects)[1] == 1
    assert store.write_batch([], rejects)[1] == 0


def test_a_reject_payload_may_be_invalid_utf8(store):
    # the column is bytea precisely so this case can be stored at all
    assert store.write_batch([], [(b"\xff\xfe", "not valid JSON", 0, 1)])[1] == 1


def test_the_database_refuses_an_implausible_heart_rate(store):
    with pytest.raises(psycopg.errors.CheckViolation):
        store.write_batch([reading(0, heart_rate=900)], [])


def test_the_database_refuses_an_unknown_classification(store):
    with pytest.raises(psycopg.errors.CheckViolation):
        store.write_batch([reading(0, hr_class="elevated")], [])


def test_a_failed_batch_leaves_nothing_behind(store):
    with pytest.raises(psycopg.errors.CheckViolation):
        store.write_batch([reading(0), reading(1, heart_rate=900)], [])
    assert count("heartbeat_readings") == 0


def test_the_store_still_works_after_a_failed_batch(store):
    with pytest.raises(psycopg.errors.CheckViolation):
        store.write_batch([reading(0, heart_rate=900)], [])
    assert store.write_batch([reading(1)], [])[0] == 1


def test_ingested_at_is_stamped_by_the_database(store):
    store.write_batch([reading(0)], [])
    with psycopg.connect(DSN, autocommit=True) as conn:
        stamped = conn.execute(
            "SELECT ingested_at IS NOT NULL AND ingested_at >= event_time"
            " FROM heartbeat_readings"
        ).fetchone()[0]
    assert stamped
