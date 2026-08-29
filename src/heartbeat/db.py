from __future__ import annotations

from datetime import datetime
from typing import Sequence

import psycopg

_INSERT_READING = """
    INSERT INTO heartbeat_readings
        (event_id, customer_id, event_time, heart_rate, hr_class,
         kafka_partition, kafka_offset)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
"""

_INSERT_REJECT = """
    INSERT INTO heartbeat_rejects (raw_payload, reason, kafka_partition, kafka_offset)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""

Reading = tuple[str, str, datetime, int, str, int, int]
Reject = tuple[bytes | None, str, int, int]


class HeartbeatStore:
    def __init__(self, dsn: str) -> None:
        self._conn = psycopg.connect(dsn, autocommit=False)

    def write_batch(
        self, readings: Sequence[Reading], rejects: Sequence[Reject]
    ) -> tuple[int, int]:
        """Persist one poll's worth of messages atomically.

        Good rows and rejects go in the same transaction, so a batch can never
        land half-written and leave the committed offset lying about what was
        stored. Returns rows actually inserted; the shortfall is redelivery
        absorbed by the conflict clauses.
        """
        inserted = rejected = 0
        with self._conn.transaction(), self._conn.cursor() as cur:
            if readings:
                cur.executemany(_INSERT_READING, readings)
                inserted = cur.rowcount
            if rejects:
                cur.executemany(_INSERT_REJECT, rejects)
                rejected = cur.rowcount
        return inserted, rejected

    def close(self) -> None:
        self._conn.close()
