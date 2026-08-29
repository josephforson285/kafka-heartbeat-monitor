-- Heart beat monitoring store. Applied by `heartbeat init-db`; safe to re-run.

CREATE TABLE IF NOT EXISTS heartbeat_readings (
    event_id        uuid        PRIMARY KEY,
    customer_id     text        NOT NULL,
    event_time      timestamptz NOT NULL,
    heart_rate      smallint    NOT NULL,
    hr_class        text        NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,

    -- mirrors heart_rate.min/max_plausible in config.yaml; a last line of defence
    -- if a consumer ever skips validation
    CONSTRAINT heart_rate_plausible CHECK (heart_rate BETWEEN 20 AND 250),
    CONSTRAINT hr_class_known CHECK (
        hr_class IN ('bradycardia', 'normal', 'tachycardia', 'critical')
    )
);

COMMENT ON COLUMN heartbeat_readings.event_id IS
    'Producer-generated. Primary key, so redelivery is absorbed rather than duplicated.';
COMMENT ON COLUMN heartbeat_readings.ingested_at IS
    'Write time. ingested_at - event_time is end-to-end pipeline latency.';

-- "readings for customer X over window W", the query the dashboard actually runs
CREATE INDEX IF NOT EXISTS idx_readings_customer_time
    ON heartbeat_readings (customer_id, event_time DESC);

-- whole-fleet time range scans
CREATE INDEX IF NOT EXISTS idx_readings_time
    ON heartbeat_readings (event_time DESC);

-- alert panels only ever look at the abnormal rows
CREATE INDEX IF NOT EXISTS idx_readings_alerting
    ON heartbeat_readings (event_time DESC)
    WHERE hr_class <> 'normal';


-- Rejected messages. Bad data stays visible instead of being swallowed.
CREATE TABLE IF NOT EXISTS heartbeat_rejects (
    reject_id       bigserial   PRIMARY KEY,
    -- bytea, not text: a payload can fail precisely because it is not valid UTF-8
    raw_payload     bytea,
    reason          text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    rejected_at     timestamptz NOT NULL DEFAULT now(),

    -- replaying a partition must not pile up duplicate rejects either
    CONSTRAINT reject_source_unique UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_rejects_time
    ON heartbeat_rejects (rejected_at DESC);
