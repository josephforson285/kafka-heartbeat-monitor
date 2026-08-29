"""Synthetic heart rate readings for a fleet of fake customers."""

from __future__ import annotations

import random
from dataclasses import dataclass

from .config import GeneratorSettings, HeartRateBands
from .schema import HeartbeatEvent

# what a broken sensor actually emits: dead channel, rail, sign error
_SENSOR_FAULTS = (0, -1, 300, 511, 999)


@dataclass(slots=True)
class _Customer:
    customer_id: str
    baseline: int
    current: int


class HeartRateGenerator:
    """A random walk per customer, with sensor faults and clinical anomalies mixed in.

    Seeded, so a run can be reproduced when a test disagrees with you.
    """

    def __init__(self, settings: GeneratorSettings, bands: HeartRateBands) -> None:
        self._settings = settings
        self._bands = bands
        self._rng = random.Random(settings.seed)
        self._customers = []
        for number in range(1, settings.customers + 1):
            baseline = self._rng.randint(58, 88)
            self._customers.append(_Customer(f"cust-{number:04d}", baseline, baseline))

    def next_reading(self) -> HeartbeatEvent:
        customer = self._rng.choice(self._customers)
        roll = self._rng.random()
        if roll < self._settings.fault_rate:
            heart_rate = self._rng.choice(_SENSOR_FAULTS)
        elif roll < self._settings.fault_rate + self._settings.anomaly_rate:
            heart_rate = self._alerting()
        else:
            heart_rate = self._walk(customer)
        return HeartbeatEvent.new(customer.customer_id, heart_rate)

    def _walk(self, customer: _Customer) -> int:
        # drift plus a pull back toward baseline, so the walk cannot wander off
        drift = self._rng.randint(-4, 4)
        pull = (customer.baseline - customer.current) // 4
        customer.current = max(48, min(115, customer.current + drift + pull))
        return customer.current

    def _alerting(self) -> int:
        """A reading that is medically real but clinically abnormal — not a fault."""
        if self._rng.random() < 0.4:
            return self._rng.randint(
                self._bands.min_plausible, self._bands.bradycardia_below - 1
            )
        return self._rng.randint(
            self._bands.tachycardia_above + 1, self._bands.max_plausible
        )
