import pytest

from heartbeat.config import (
    Config,
    ConfigError,
    ConsumerSettings,
    GeneratorSettings,
    HeartRateBands,
    TopicSpec,
)


def bands(**overrides) -> HeartRateBands:
    defaults = dict(
        min_plausible=20,
        max_plausible=250,
        bradycardia_below=60,
        tachycardia_above=100,
        critical_above=180,
    )
    return HeartRateBands(**{**defaults, **overrides})


def generator(**overrides) -> GeneratorSettings:
    defaults = dict(
        customers=50, readings_per_second=25, seed=42, fault_rate=0.02, anomaly_rate=0.05
    )
    return GeneratorSettings(**{**defaults, **overrides})


def test_valid_bands_are_accepted():
    assert bands().critical_above == 180


def test_bands_out_of_order_are_rejected():
    with pytest.raises(ConfigError, match="ascending order"):
        bands(critical_above=90)


def test_critical_threshold_below_tachycardia_is_rejected():
    with pytest.raises(ConfigError, match="ascending order"):
        bands(tachycardia_above=200)


@pytest.mark.parametrize("rate", [-0.1, 1.5])
def test_rates_outside_zero_to_one_are_rejected(rate):
    with pytest.raises(ConfigError, match="between 0 and 1"):
        generator(fault_rate=rate)


def test_rates_cannot_sum_above_one():
    with pytest.raises(ConfigError, match="cannot exceed 1"):
        generator(fault_rate=0.7, anomaly_rate=0.5)


def test_customers_must_be_positive():
    with pytest.raises(ConfigError, match="at least 1"):
        generator(customers=0)


def test_reading_rate_must_be_positive():
    with pytest.raises(ConfigError, match="must be positive"):
        generator(readings_per_second=0)


def test_batch_size_must_be_positive():
    with pytest.raises(ConfigError, match="at least 1"):
        ConsumerSettings(batch_size=0, batch_timeout_seconds=2.0)


def test_batch_timeout_must_be_positive():
    with pytest.raises(ConfigError, match="must be positive"):
        ConsumerSettings(batch_size=500, batch_timeout_seconds=0)


def test_a_replicated_topic_is_accepted():
    spec = TopicSpec("t", partitions=3, replication_factor=3, min_insync_replicas=2)
    assert spec.min_insync_replicas == 2


def test_min_insync_above_replication_factor_is_rejected():
    # such a topic can never accept a write, so catch it at load rather than at produce
    with pytest.raises(ConfigError, match="exceeds replication_factor"):
        TopicSpec("t", partitions=3, replication_factor=1, min_insync_replicas=2)


def test_min_insync_equal_to_replication_factor_is_allowed():
    # legal, but one broker loss stops writes; that is the operator's call, not ours
    assert TopicSpec("t", 1, replication_factor=3, min_insync_replicas=3)


def topic() -> TopicSpec:
    return TopicSpec("heartbeat.raw", partitions=3, replication_factor=3)


def test_a_matching_topic_reports_no_mismatch():
    assert topic().mismatch(partitions=3, replication_factor=3) is None


def test_an_unreplicated_topic_is_reported():
    # the real case: auto-creation had already made the topic at RF=1
    problem = topic().mismatch(partitions=3, replication_factor=1)
    assert "replication factor 1" in problem
    assert "config says 3" in problem


def test_a_wrong_partition_count_is_reported():
    assert "1 partitions" in topic().mismatch(partitions=1, replication_factor=3)


def test_both_problems_are_reported_together():
    problem = topic().mismatch(partitions=1, replication_factor=1)
    assert "partitions" in problem and "replication factor" in problem


VALID_CONFIG = """
kafka:
  bootstrap_servers: localhost:9092
  topics:
    raw: {name: heartbeat.raw, partitions: 3, replication_factor: 3, min_insync_replicas: 2}
    dlq: {name: heartbeat.dlq, partitions: 1, replication_factor: 3, min_insync_replicas: 2}
generator: {customers: 5, readings_per_second: 10, seed: 1, fault_rate: 0.02, anomaly_rate: 0.05}
heart_rate: {min_plausible: 20, max_plausible: 250, bradycardia_below: 60,
             tachycardia_above: 100, critical_above: 180}
consumer: {batch_size: 100, batch_timeout_seconds: 1.0}
database: {host: localhost, port: 5434, name: heartbeat}
"""


@pytest.fixture
def credentials(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "user"), monkeypatch.setenv("POSTGRES_PASSWORD", "pw")


def config_file(tmp_path, text=VALID_CONFIG):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


def test_a_valid_file_loads(tmp_path, credentials):
    config = Config.load(config_file(tmp_path))
    assert config.raw_topic.partitions == 3
    assert config.bands.critical_above == 180


def test_the_dsn_is_assembled_from_file_and_environment(tmp_path, credentials):
    dsn = Config.load(config_file(tmp_path)).dsn
    assert "port=5434" in dsn and "dbname=heartbeat" in dsn and "user=user" in dsn


def test_a_missing_file_is_reported(tmp_path, credentials):
    with pytest.raises(ConfigError, match="config file not found"):
        Config.load(tmp_path / "absent.yaml")


def test_a_missing_section_is_reported(tmp_path, credentials):
    text = VALID_CONFIG.replace("consumer: {batch_size: 100, batch_timeout_seconds: 1.0}", "")
    with pytest.raises(ConfigError, match="missing 'consumer'"):
        Config.load(config_file(tmp_path, text))


def test_a_missing_scalar_key_is_reported(tmp_path, credentials):
    # regression: this used to escape as a bare KeyError traceback
    text = VALID_CONFIG.replace("  bootstrap_servers: localhost:9092\n", "")
    with pytest.raises(ConfigError, match="missing 'kafka.bootstrap_servers'"):
        Config.load(config_file(tmp_path, text))


def test_a_missing_topic_is_reported(tmp_path, credentials):
    text = "\n".join(l for l in VALID_CONFIG.splitlines() if not l.strip().startswith("dlq:"))
    with pytest.raises(ConfigError, match="missing 'kafka.topics.dlq'"):
        Config.load(config_file(tmp_path, text))


def test_a_missing_database_key_is_reported(tmp_path, credentials):
    text = VALID_CONFIG.replace("port: 5434, ", "")
    with pytest.raises(ConfigError, match="missing 'database.port'"):
        Config.load(config_file(tmp_path, text))


def test_a_misspelled_key_names_the_section(tmp_path, credentials):
    # a TypeError from the dataclass would not say which file or section is wrong
    text = VALID_CONFIG.replace("batch_size: 100", "batch_sixe: 100")
    with pytest.raises(ConfigError, match="'consumer'"):
        Config.load(config_file(tmp_path, text))


def test_a_section_that_is_not_a_mapping_is_reported(tmp_path, credentials):
    text = VALID_CONFIG.replace("consumer: {batch_size: 100, batch_timeout_seconds: 1.0}",
                                "consumer: 12")
    with pytest.raises(ConfigError, match="must be a mapping"):
        Config.load(config_file(tmp_path, text))


def test_validation_still_fires_through_load(tmp_path, credentials):
    text = VALID_CONFIG.replace("critical_above: 180", "critical_above: 90")
    with pytest.raises(ConfigError, match="ascending order"):
        Config.load(config_file(tmp_path, text))


def test_a_missing_credential_is_reported(monkeypatch):
    from heartbeat.config import _require_env
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    with pytest.raises(ConfigError, match="copy .env.example"):
        _require_env("POSTGRES_USER")
