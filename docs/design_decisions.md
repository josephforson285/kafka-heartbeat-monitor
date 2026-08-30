# Design decisions


### KRaft instead of ZooKeeper

Kafka 4 removed ZooKeeper, so the cluster runs the official Apache Kafka image in KRaft mode.

### `event_id` for exactly-once storage

Kafka may redeliver messages after failures. Each event gets a UUID used as the database primary key, with `ON CONFLICT DO NOTHING`, so retries do not create duplicate rows.

### Messages keyed by `customer_id`

Kafka guarantees ordering only within a partition. Keying by customer keeps each patient's readings ordered while three partitions allow consumer-group parallelism.

### Invalid data is different from an alert

A plausible but dangerous reading such as 190 bpm is stored and classified. Impossible or malformed data, such as 900 bpm or missing fields, is rejected with its reason, partition and offset.

### Classifications are ranges, not diagnoses

`bradycardia`, `tachycardia` and `critical` are fixed thresholds. They do not account for age, activity, medication or individual baselines.


### Three brokers make replication real

With RF=3 and `min.insync.replicas=2`, writes require two copies and one broker can fail without stopping ingestion. Internal Kafka topics are replicated too.

### Contract changes fail differently

Adding unknown fields is backward compatible. Missing, renamed or mistyped fields are rejected. Semantic changes are more dangerous because structurally valid but incorrect data can still be stored.

<!-- ### Kafka data must use the mounted volume

The Apache image does not automatically use the mounted data path. `KAFKA_LOG_DIRS` points Kafka to persistent storage so topics survive container recreation. -->

### Topics are created explicitly

Automatic topic creation is disabled. `create-topics` creates or validates partition and replication settings so a typo cannot silently create an incorrectly configured topic.

### Offsets commit after database writes

A poll's readings and rejects are written in one PostgreSQL transaction before Kafka offsets advance. If the consumer crashes between them, Kafka replays the batch and `event_id` removes duplicates.

### PostgreSQL rejects are the source of truth

Rejected messages are stored transactionally in `heartbeat_rejects`. The Kafka DLQ copy is best-effort and exists mainly for convenient replay.

### Dashboard sections answer separate questions

The dashboard separates fleet health, the selected patient, patients needing attention and pipeline health.
 <!-- Panels are clearly scoped so selecting a patient does not appear to change fleet-wide metrics. -->

<!-- ### Dashboard and alert thresholds must agree

A count across a selectable time range changes simply because the window changes. Critical activity is therefore shown as a rate using the same calibration as the alert. -->
<!-- 
### Alerts use a measured baseline

Alerting on any critical reading caused permanent alerts. The fleet alert instead detects a sustained rate far above normal traffic, making it a surge detector rather than a patient-level clinical alarm. -->

### Missing data is also a failure

A patient with no new readings can otherwise appear healthy because old data remains visible. The dashboard tracks patients reporting recently, patients gone silent and how long each has been silent.

<!-- ### One reading does not define a patient's condition

Ranking patients by their latest reading was unstable. Attention ranking therefore uses episodes over a time window instead of one sample.

Bradycardia is excluded from ranking because fixed low-heart-rate thresholds cannot distinguish deterioration from a naturally low baseline. -->

<!-- ### Fleet metrics count the useful thing

Because the generator distributes anomalies across all patients, counting affected patients gives little information. The fleet tile therefore counts abnormal readings and clearly states what it measures. -->

<!-- ### Patient charts show raw readings

Averages can hide both extreme highs and lows. Since each patient generates a manageable number of points, the chart plots every reading and shows highest, lowest and average separately. -->

<!-- ### Grafana is provisioned from files

Dashboards, datasource configuration and alerts live in the repository so a fresh clone starts with the same monitoring setup. -->

<!-- ### Deliberately excluded

Schema Registry/Avro, Prometheus exporters and Spark are unnecessary for the current scale. They could be added later, but would add complexity without solving a current requirement. -->
