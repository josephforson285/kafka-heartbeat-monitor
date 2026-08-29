import json
from datetime import datetime, timezone

import pytest

from heartbeat.schema import HeartbeatEvent, SchemaError

VALID = {
    "schema_version": 1,
    "event_id": "3f1b6d84-0f1a-4a1e-9a5e-1c2d3e4f5a6b",
    "customer_id": "cust-0001",
    "event_time": "2026-08-29T01:02:03+00:00",
    "heart_rate": 72,
}


def payload(**overrides) -> bytes:
    return json.dumps({**VALID, **overrides}).encode()


def test_roundtrip_preserves_the_event():
    event = HeartbeatEvent.new("cust-0001", 72)
    assert HeartbeatEvent.from_json(event.to_json()) == event


def test_new_stamps_an_aware_timestamp_and_unique_id():
    first = HeartbeatEvent.new("cust-0001", 72)
    second = HeartbeatEvent.new("cust-0001", 72)
    assert first.event_id != second.event_id
    assert first.event_time.tzinfo is not None


def test_key_is_the_customer_id():
    assert HeartbeatEvent.new("cust-0042", 80).key == b"cust-0042"


@pytest.mark.parametrize(
    "raw, expected",
    [
        (b"not json", "not valid JSON"),
        (b"[1, 2, 3]", "expected a JSON object"),
        (b"\xff\xfe{}", "not valid JSON"),
    ],
)
def test_malformed_payloads_are_rejected(raw, expected):
    with pytest.raises(SchemaError, match=expected):
        HeartbeatEvent.from_json(raw)


def test_missing_fields_are_named_in_the_error():
    with pytest.raises(SchemaError, match="event_time, heart_rate"):
        HeartbeatEvent.from_json(json.dumps({"event_id": VALID["event_id"],
                                             "customer_id": "c"}).encode())


def test_booleans_are_not_accepted_as_heart_rates():
    # bool subclasses int, so this passes a naive isinstance check
    with pytest.raises(SchemaError, match="heart_rate must be an integer"):
        HeartbeatEvent.from_json(payload(heart_rate=True))


@pytest.mark.parametrize("value", ["seventy", 72.5, None])
def test_non_integer_heart_rates_are_rejected(value):
    with pytest.raises(SchemaError, match="heart_rate must be an integer"):
        HeartbeatEvent.from_json(payload(heart_rate=value))


def test_naive_timestamps_are_rejected():
    with pytest.raises(SchemaError, match="must carry a UTC offset"):
        HeartbeatEvent.from_json(payload(event_time="2026-08-29T01:02:03"))


def test_unparseable_timestamps_are_rejected():
    with pytest.raises(SchemaError, match="not ISO-8601"):
        HeartbeatEvent.from_json(payload(event_time="yesterday"))


def test_event_id_must_be_a_uuid():
    with pytest.raises(SchemaError, match="not a UUID"):
        HeartbeatEvent.from_json(payload(event_id="not-a-uuid"))


@pytest.mark.parametrize("field", ["event_id", "customer_id"])
def test_empty_identifiers_are_rejected(field):
    with pytest.raises(SchemaError, match=f"{field} must be a non-empty string"):
        HeartbeatEvent.from_json(payload(**{field: ""}))


def test_unknown_schema_version_is_carried_not_rejected():
    # adding an optional field is backward compatible; the consumer keeps working
    event = HeartbeatEvent.from_json(payload(schema_version=2, extra="ignored"))
    assert event.schema_version == 2
    assert event.heart_rate == 72


def test_timestamp_survives_the_roundtrip_exactly():
    when = datetime(2026, 8, 29, 1, 2, 3, tzinfo=timezone.utc)
    event = HeartbeatEvent.new("cust-0001", 72, event_time=when)
    assert HeartbeatEvent.from_json(event.to_json()).event_time == when
