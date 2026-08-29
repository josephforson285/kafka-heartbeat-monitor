"""The event contract shared by the producer and the consumer.

Shape and serialisation only. Whether a reading is medically plausible or
clinically alerting is decided in `validation`.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

SCHEMA_VERSION = 1

REQUIRED_FIELDS = frozenset({"event_id", "customer_id", "event_time", "heart_rate"})


class SchemaError(ValueError):
    """Payload does not conform to the contract. The message is DLQ-ready."""


@dataclass(frozen=True, slots=True)
class HeartbeatEvent:
    event_id: str
    customer_id: str
    event_time: datetime
    heart_rate: int
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def new(
        cls,
        customer_id: str,
        heart_rate: int,
        event_time: datetime | None = None,
    ) -> HeartbeatEvent:
        return cls(
            event_id=str(uuid.uuid4()),
            customer_id=customer_id,
            event_time=event_time or datetime.now(timezone.utc),
            heart_rate=heart_rate,
        )

    @property
    def key(self) -> bytes:
        """Partition key. Keying by customer keeps one patient's readings ordered."""
        return self.customer_id.encode()

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "event_id": self.event_id,
                "customer_id": self.customer_id,
                "event_time": self.event_time.isoformat(),
                "heart_rate": self.heart_rate,
            }
        ).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> HeartbeatEvent:
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaError(f"not valid JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise SchemaError(f"expected a JSON object, got {type(payload).__name__}")

        missing = REQUIRED_FIELDS - payload.keys()
        if missing:
            raise SchemaError(f"missing field(s): {', '.join(sorted(missing))}")

        for field in ("event_id", "customer_id"):
            if not isinstance(payload[field], str) or not payload[field]:
                raise SchemaError(f"{field} must be a non-empty string")

        heart_rate = payload["heart_rate"]
        # bool subclasses int, so True would otherwise sail through as a heart rate
        if isinstance(heart_rate, bool) or not isinstance(heart_rate, int):
            raise SchemaError(f"heart_rate must be an integer, got {heart_rate!r}")

        try:
            event_time = datetime.fromisoformat(payload["event_time"])
        except (TypeError, ValueError) as exc:
            raise SchemaError(
                f"event_time is not ISO-8601: {payload['event_time']!r}"
            ) from exc
        if event_time.tzinfo is None:
            raise SchemaError("event_time must carry a UTC offset")

        return cls(
            event_id=payload["event_id"],
            customer_id=payload["customer_id"],
            event_time=event_time,
            heart_rate=heart_rate,
            schema_version=payload.get("schema_version", SCHEMA_VERSION),
        )
