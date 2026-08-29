"""Is this reading physically possible, and if so, is the patient in trouble?

Two different questions. A heart rate of 190 is real data about someone who needs
attention; a heart rate of 900 is a broken sensor. Dropping the first because it
looks extreme would discard the event the system exists to catch.
"""

from __future__ import annotations

from enum import StrEnum

from .config import HeartRateBands
from .schema import HeartbeatEvent


class HeartRateClass(StrEnum):
    BRADYCARDIA = "bradycardia"
    NORMAL = "normal"
    TACHYCARDIA = "tachycardia"
    CRITICAL = "critical"


class ImplausibleReading(ValueError):
    """Not a possible human heart rate. Sensor fault, not a patient event."""


def classify(event: HeartbeatEvent, bands: HeartRateBands) -> HeartRateClass:
    rate = event.heart_rate
    if not bands.min_plausible <= rate <= bands.max_plausible:
        raise ImplausibleReading(
            f"heart_rate {rate} outside plausible range "
            f"{bands.min_plausible}-{bands.max_plausible}"
        )
    if rate < bands.bradycardia_below:
        return HeartRateClass.BRADYCARDIA
    # critical first: the critical threshold sits above the tachycardia one
    if rate > bands.critical_above:
        return HeartRateClass.CRITICAL
    if rate > bands.tachycardia_above:
        return HeartRateClass.TACHYCARDIA
    return HeartRateClass.NORMAL
