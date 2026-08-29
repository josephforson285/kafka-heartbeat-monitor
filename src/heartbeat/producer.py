from __future__ import annotations

import logging
import time

from confluent_kafka import Producer

from .config import Config
from .generator import HeartRateGenerator
from .runtime import shutdown_on_signal
from .schema import HeartbeatEvent

log = logging.getLogger(__name__)


class HeartbeatProducer:
    """Wraps the Kafka producer and keeps score of what actually got delivered.

    produce() only enqueues locally; delivery is reported later on a callback, so
    `delivered` is the only number that means anything.
    """

    def __init__(self, config: Config) -> None:
        # hand librdkafka our logger, or its C-level messages go straight to
        # stderr unformatted and read like crashes next to our own output
        self._producer = Producer({**config.producer_config(), "logger": log})
        self._topic = config.raw_topic.name
        self.delivered = 0
        self.failed = 0

    def send(self, event: HeartbeatEvent) -> None:
        payload = event.to_json()
        try:
            self._enqueue(event.key, payload)
        except BufferError:
            # local queue is full; serving callbacks is what frees space
            self._producer.poll(1.0)
            self._enqueue(event.key, payload)
        self._producer.poll(0)

    def _enqueue(self, key: bytes, payload: bytes) -> None:
        self._producer.produce(
            self._topic, key=key, value=payload, on_delivery=self._on_delivery
        )

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            log.error("delivery failed (partition %s): %s", msg.partition(), err)

    def close(self, timeout: float = 30.0) -> int:
        unflushed = self._producer.flush(timeout)
        if unflushed:
            log.error("%d message(s) unflushed after %.0fs", unflushed, timeout)
        return unflushed


def run_producer(
    config: Config,
    *,
    count: int | None = None,
    duration: float | None = None,
    rate: float | None = None,
) -> int:
    generator = HeartRateGenerator(config.generator, config.bands)
    producer = HeartbeatProducer(config)
    readings_per_second = rate or config.generator.readings_per_second
    interval = 1.0 / readings_per_second

    log.info(
        "producing to %s at %.1f readings/s across %d customers",
        config.raw_topic.name,
        readings_per_second,
        config.generator.customers,
    )

    started = time.monotonic()
    due = started
    sent = 0
    # flush in a finally: produce() only buffers locally, so failing out of the loop
    # without it discards whatever had not reached a broker yet, silently
    try:
        with shutdown_on_signal() as stop:
            while not stop:
                if count is not None and sent >= count:
                    break
                if duration is not None and time.monotonic() - started >= duration:
                    break

                producer.send(generator.next_reading())
                sent += 1

                # schedule against a fixed clock so send latency does not drift the rate
                due += interval
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            if stop:
                log.info("shutdown requested, flushing")
    finally:
        unflushed = producer.close()
        log.info(
            "sent=%d delivered=%d failed=%d unflushed=%d",
            sent,
            producer.delivered,
            producer.failed,
            unflushed,
        )
    return 1 if producer.failed or unflushed else 0
