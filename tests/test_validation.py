import pytest

from heartbeat.config import HeartRateBands
from heartbeat.schema import HeartbeatEvent
from heartbeat.validation import HeartRateClass, ImplausibleReading, classify

BANDS = HeartRateBands(
    min_plausible=20,
    max_plausible=250,
    bradycardia_below=60,
    tachycardia_above=100,
    critical_above=180,
)


def reading(heart_rate: int) -> HeartbeatEvent:
    return HeartbeatEvent.new("cust-0001", heart_rate)


@pytest.mark.parametrize(
    "heart_rate, expected",
    [
        (20, HeartRateClass.BRADYCARDIA),
        (59, HeartRateClass.BRADYCARDIA),
        (60, HeartRateClass.NORMAL),
        (100, HeartRateClass.NORMAL),
        (101, HeartRateClass.TACHYCARDIA),
        (180, HeartRateClass.TACHYCARDIA),
        (181, HeartRateClass.CRITICAL),
        (250, HeartRateClass.CRITICAL),
    ],
)
def test_band_boundaries(heart_rate, expected):
    assert classify(reading(heart_rate), BANDS) is expected


@pytest.mark.parametrize("heart_rate", [-1, 0, 19, 251, 999])
def test_impossible_readings_are_sensor_faults(heart_rate):
    with pytest.raises(ImplausibleReading, match="outside plausible range"):
        classify(reading(heart_rate), BANDS)


def test_an_alarming_but_possible_reading_is_kept():
    # the point of the whole system: 190 bpm is a patient in trouble, not bad data
    assert classify(reading(190), BANDS) is HeartRateClass.CRITICAL


def test_class_values_match_the_database_constraint():
    assert {c.value for c in HeartRateClass} == {
        "bradycardia",
        "normal",
        "tachycardia",
        "critical",
    }
