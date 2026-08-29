"""Single entrypoint for every stage of the pipeline. All stages are re-runnable."""

from __future__ import annotations

import argparse
import logging

import psycopg
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
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)

    missing = []
    for spec in (config.raw_topic, config.dlq_topic):
        if spec.name in existing:
            log.info("topic %s already exists", spec.name)
        else:
            missing.append(spec)
    if not missing:
        return 0

    futures = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication_factor,
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


def cmd_produce(config: Config, args: argparse.Namespace) -> int:
    return run_producer(config, count=args.count, duration=args.duration)


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

    create_topics = sub.add_parser("create-topics", help="create the Kafka topics")
    create_topics.set_defaults(handler=cmd_create_topics)

    produce = sub.add_parser("produce", help="stream synthetic readings into Kafka")
    produce.add_argument("--count", type=int, help="stop after N readings")
    produce.add_argument("--duration", type=float, help="stop after N seconds")
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
    return args.handler(config, args)


if __name__ == "__main__":
    raise SystemExit(main())
