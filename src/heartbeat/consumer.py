from __future__ import annotations

import logging
from dataclasses import dataclass

from confluent_kafka import Consumer, Message, Producer

from .config import Config
from .db import HeartbeatStore
from .runtime import shutdown_on_signal
from .schema import HeartbeatEvent, SchemaError
from .validation import ImplausibleReading, classify

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Stats:
    consumed: int = 0
    inserted: int = 0
    duplicates: int = 0
    rejected: int = 0
    broker_errors: int = 0


def run_consumer(
    config: Config,
    *,
    group_id: str | None = None,
    max_messages: int | None = None,
    drain: bool = False,
) -> int:
    options = config.consumer_config(group_id)
    consumer = Consumer({**options, "logger": log})
    assignment: set[int] = set()

    def on_assign(_consumer: Consumer, partitions) -> None:
        assignment.update(p.partition for p in partitions)
        log.info("partitions assigned: %s", sorted(p.partition for p in partitions))

    def on_revoke(_consumer: Consumer, partitions) -> None:
        assignment.difference_update(p.partition for p in partitions)
        log.info("partitions revoked: %s", sorted(p.partition for p in partitions))

    consumer.subscribe([config.raw_topic.name], on_assign=on_assign, on_revoke=on_revoke)
    store = HeartbeatStore(config.dsn)
    dlq = Producer({**config.producer_config(), "logger": log})
    stats = Stats()

    log.info(
        "consuming %s as group %s", config.raw_topic.name, options["group.id"]
    )

    idle_polls = 0
    # close in a finally: leaving the group cleanly on the way out of an error is
    # what stops the next consumer waiting out session.timeout.ms for our partitions
    try:
        with shutdown_on_signal() as stop:
            while not stop:
                messages = consumer.consume(
                    config.consumer.batch_size, config.consumer.batch_timeout_seconds
                )
                if not messages:
                    idle_polls += 1
                    # an empty poll before the group has given us anything means the
                    # rebalance is still pending, not that the topic is drained. A
                    # consumer killed with SIGKILL holds its partitions until the
                    # broker's session timeout expires.
                    if drain and assignment and idle_polls > 1:
                        log.info("no messages left, stopping")
                        break
                    if not assignment and idle_polls % 5 == 0:
                        log.info("waiting for a partition assignment")
                    continue

                idle_polls = 0
                readings, rejects, poison = _sort_batch(messages, config, stats)
                inserted, rejected = store.write_batch(readings, rejects)

                # offsets move only now. The rows are durable, so a crash before
                # this point replays the batch rather than losing it.
                consumer.commit(asynchronous=False)

                stats.inserted += inserted
                stats.duplicates += len(readings) - inserted
                stats.rejected += rejected
                _forward_to_dlq(dlq, config.dlq_topic.name, poison)

                if max_messages is not None and stats.consumed >= max_messages:
                    break

            if stop:
                log.info("shutdown requested")
    finally:
        undelivered = dlq.flush(10)
        if undelivered:
            log.warning("%d dead-letter replay copies unflushed", undelivered)
        consumer.close()
        store.close()
        log.info(
            "consumed=%d inserted=%d duplicates=%d rejected=%d",
            stats.consumed,
            stats.inserted,
            stats.duplicates,
            stats.rejected,
        )

    if stats.broker_errors:
        log.error("%d broker error(s) during the run", stats.broker_errors)
        return 1
    return 0


def _sort_batch(messages: list[Message], config: Config, stats: Stats):
    readings, rejects, poison = [], [], []
    for message in messages:
        if message.error():
            log.error("broker error: %s", message.error())
            stats.broker_errors += 1
            continue
        stats.consumed += 1
        try:
            event = HeartbeatEvent.from_json(message.value())
            hr_class = classify(event, config.bands)
        except (SchemaError, ImplausibleReading) as exc:
            rejects.append(
                (message.value(), str(exc), message.partition(), message.offset())
            )
            poison.append(message)
            continue
        readings.append(
            (
                event.event_id,
                event.customer_id,
                event.event_time,
                event.heart_rate,
                hr_class.value,
                message.partition(),
                message.offset(),
            )
        )
    return readings, rejects, poison


def _forward_to_dlq(producer: Producer, topic: str, messages: list[Message]) -> None:
    """Best effort only. heartbeat_rejects is the record of truth; this topic exists
    so a fixed consumer can replay poison messages without re-reading the main log."""
    for message in messages:
        try:
            producer.produce(topic, key=message.key(), value=message.value())
        except BufferError:
            log.warning("dlq queue full, replay copy dropped")
    producer.poll(0)
