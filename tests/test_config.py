import pytest

from heartbeat.config import (
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
