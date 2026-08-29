"""Typed configuration: parameters from config.yaml, secrets from .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SCHEMA_SQL_PATH = PROJECT_ROOT / "sql" / "001_schema.sql"


class ConfigError(RuntimeError):
    """Configuration is missing or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int


@dataclass(frozen=True, slots=True)
class HeartRateBands:
    min_plausible: int
    max_plausible: int
    bradycardia_below: int
    tachycardia_above: int
    critical_above: int

    def __post_init__(self) -> None:
        ordered = [
            self.min_plausible,
            self.bradycardia_below,
            self.tachycardia_above,
            self.critical_above,
            self.max_plausible,
        ]
        if ordered != sorted(ordered):
            raise ConfigError(f"heart_rate bands are not in ascending order: {ordered}")


@dataclass(frozen=True, slots=True)
class GeneratorSettings:
    customers: int
    readings_per_second: float
    seed: int
    fault_rate: float
    anomaly_rate: float

    def __post_init__(self) -> None:
        if self.customers < 1:
            raise ConfigError("generator.customers must be at least 1")
        if self.readings_per_second <= 0:
            raise ConfigError("generator.readings_per_second must be positive")
        for name in ("fault_rate", "anomaly_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ConfigError(f"generator.{name} must be between 0 and 1, got {value}")
        if self.fault_rate + self.anomaly_rate > 1.0:
            raise ConfigError("generator fault_rate + anomaly_rate cannot exceed 1")


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    batch_size: int
    batch_timeout_seconds: float

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ConfigError("consumer.batch_size must be at least 1")
        if self.batch_timeout_seconds <= 0:
            raise ConfigError("consumer.batch_timeout_seconds must be positive")


@dataclass(frozen=True, slots=True)
class Config:
    bootstrap_servers: str
    raw_topic: TopicSpec
    dlq_topic: TopicSpec
    generator: GeneratorSettings
    bands: HeartRateBands
    consumer: ConsumerSettings
    dsn: str
    _producer_options: dict[str, Any] = field(default_factory=dict)
    _consumer_options: dict[str, Any] = field(default_factory=dict)

    def producer_config(self) -> dict[str, Any]:
        return {"bootstrap.servers": self.bootstrap_servers, **self._producer_options}

    def consumer_config(self, group_id: str | None = None) -> dict[str, Any]:
        options = {"bootstrap.servers": self.bootstrap_servers, **self._consumer_options}
        if group_id:
            options["group.id"] = group_id
        return options

    @classmethod
    def load(cls, path: Path | str = DEFAULT_CONFIG_PATH) -> Config:
        path = Path(path)
        if not path.is_file():
            raise ConfigError(f"config file not found: {path}")
        raw = yaml.safe_load(path.read_text()) or {}

        load_dotenv(PROJECT_ROOT / ".env")
        user = _require_env("POSTGRES_USER")
        password = _require_env("POSTGRES_PASSWORD")

        kafka = _section(raw, "kafka", path)
        topics = _section(kafka, "topics", path, parent="kafka")
        database = _section(raw, "database", path)

        return cls(
            bootstrap_servers=kafka["bootstrap_servers"],
            raw_topic=TopicSpec(**topics["raw"]),
            dlq_topic=TopicSpec(**topics["dlq"]),
            generator=GeneratorSettings(**_section(raw, "generator", path)),
            bands=HeartRateBands(**_section(raw, "heart_rate", path)),
            consumer=ConsumerSettings(**_section(raw, "consumer", path)),
            dsn=(
                f"host={database['host']} port={database['port']} "
                f"dbname={database['name']} user={user} password={password}"
            ),
            _producer_options=dict(kafka.get("producer", {})),
            _consumer_options=dict(kafka.get("consumer", {})),
        )


def _section(source: dict, key: str, path: Path, parent: str = "") -> dict:
    if key not in source:
        location = f"{parent}.{key}" if parent else key
        raise ConfigError(f"missing '{location}' section in {path}")
    return source[key]


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ConfigError(f"{name} is not set; copy .env.example to .env and fill it in")
    return value
