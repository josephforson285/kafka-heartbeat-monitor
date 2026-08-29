"""Single entrypoint for every stage of the pipeline. All stages are re-runnable."""

from __future__ import annotations

import argparse
import logging

import psycopg
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .config import SCHEMA_SQL_PATH, Config, ConfigError, DEFAULT_CONFIG_PATH
from .consumer import run_consumer
from .logging_conf import configure
from .producer import run_producer

log = logging.getLogger("heartbeat.cli")


def cmd_init_db(config: Config, _args: argparse.Namespace) -> int:
    with psycopg.connect(config.dsn) as conn:
        conn.execute(SCHEMA_SQL_PATH.read_text())
    log.info("applied %s", SCHEMA_SQL_PATH.name)
    return 0


def cmd_create_topics(config: Config, _args: argparse.Namespace) -> int:
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers, "logger": log})
    existing = admin.list_topics(timeout=10).topics

    missing = []
    mismatched = 0
    for spec in (config.raw_topic, config.dlq_topic):
        topic = existing.get(spec.name)
        if topic is None:
            missing.append(spec)
            continue
        replication = max((len(p.replicas) for p in topic.partitions.values()), default=0)
        problem = spec.mismatch(len(topic.partitions), replication)
        if problem:
            log.error("topic %s %s", spec.name, problem)
            mismatched += 1
        else:
            log.info("topic %s already exists and matches config", spec.name)

    if mismatched:
        log.error(
            "refusing to continue: delete the topic and re-create it, or reassign "
            "its partitions. Config claiming a durability guarantee the cluster is "
            "not providing is worse than no config at all."
        )
        return 1
    if not missing:
        return 0

    futures = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication_factor,
                config={"min.insync.replicas": str(spec.min_insync_replicas)},
            )
            for spec in missing
        ]
    )
    failures = 0
    for name, future in futures.items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as exc:
            log.error("could not create topic %s: %s", name, exc)
            failures += 1
    return 1 if failures else 0


def cmd_topic_info(config: Config, _args: argparse.Namespace) -> int:
    """Leader and in-sync replicas per partition — replication is invisible otherwise."""
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers, "logger": log})
    metadata = admin.list_topics(timeout=10)

    print(f"brokers: {sorted(metadata.brokers)}")
    for spec in (config.raw_topic, config.dlq_topic):
        topic = metadata.topics.get(spec.name)
        if topic is None:
            print(f"\n{spec.name}: does not exist")
            continue
        print(f"\n{spec.name}")
        for number, partition in sorted(topic.partitions.items()):
            under = set(partition.replicas) - set(partition.isrs)
            flag = f"   UNDER-REPLICATED, missing {sorted(under)}" if under else ""
            print(
                f"  partition {number}"
                f"   leader {partition.leader}"
                f"   replicas {sorted(partition.replicas)}"
                f"   isr {sorted(partition.isrs)}{flag}"
            )
    return 0


def cmd_produce(config: Config, args: argparse.Namespace) -> int:
    return run_producer(
        config, count=args.count, duration=args.duration, rate=args.rate
    )


def cmd_consume(config: Config, args: argparse.Namespace) -> int:
    return run_consumer(
        config,
        group_id=args.group,
        max_messages=args.max_messages,
        drain=args.drain,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="heartbeat", description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to config.yaml")
    parser.add_argument("--log-level", default="INFO")

    sub = parser.add_subparsers(dest="command", required=True)

    init_db = sub.add_parser("init-db", help="apply the database schema")
    init_db.set_defaults(handler=cmd_init_db)

    create_topics = sub.add_parser(
        "create-topics", help="create the topics, or verify they match config"
    )
    create_topics.set_defaults(handler=cmd_create_topics)

    topic_info = sub.add_parser(
        "topic-info", help="show leader and in-sync replicas per partition"
    )
    topic_info.set_defaults(handler=cmd_topic_info)

    produce = sub.add_parser("produce", help="stream synthetic readings into Kafka")
    produce.add_argument("--count", type=int, help="stop after N readings")
    produce.add_argument("--duration", type=float, help="stop after N seconds")
    produce.add_argument("--rate", type=float, help="override readings per second")
    produce.set_defaults(handler=cmd_produce)

    consume = sub.add_parser("consume", help="write readings from Kafka into PostgreSQL")
    consume.add_argument("--group", help="override the consumer group id")
    consume.add_argument("--max-messages", type=int, help="stop after N messages")
    consume.add_argument(
        "--drain", action="store_true", help="stop once the topic is caught up"
    )
    consume.set_defaults(handler=cmd_consume)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure(args.log_level)
    try:
        config = Config.load(args.config)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2

    # a missing stack is the likeliest way this is run wrong; say so instead of
    # printing a driver traceback
    try:
        return args.handler(config, args)
    except psycopg.OperationalError as exc:
        _unreachable("PostgreSQL", str(exc).strip().splitlines()[0])
        return 3
    except KafkaException as exc:
        _unreachable("Kafka", _kafka_detail(exc))
        return 3


def _kafka_detail(exc: KafkaException) -> str:
    error = exc.args[0] if exc.args else None
    return error.str() if hasattr(error, "str") else str(exc)


def _unreachable(service: str, detail: str) -> None:
    log.error("cannot reach %s: %s", service, detail)
    log.error("is the stack running? start it with: docker compose up -d")


if __name__ == "__main__":
    raise SystemExit(main())
