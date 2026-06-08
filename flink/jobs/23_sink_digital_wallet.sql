-- =============================================================================
-- 23_sink_digital_wallet.sql
-- Kafka topic (raw oplog CDC) → Iceberg bronze.meeza_digital_wallet_events
-- Watermark on event_ts (10 s budget — mobile clients can lag slightly);
-- upsert keyed on _id.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS polaris WITH (
    'type'                                 = 'iceberg',
    'catalog-impl'                         = 'org.apache.iceberg.rest.RESTCatalog',
    'uri'                                  = 'http://polaris:8181/api/catalog',
    'warehouse'                            = 'ebc_lakehouse',
    'credential'                           = 'root:s3cr3t',
    'scope'                                = 'PRINCIPAL_ROLE:ALL',
    'token-refresh-enabled'                = 'true',
    'io-impl'                              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'                          = 'http://minio:9000',
    's3.access-key-id'                     = 'minioadmin',
    's3.secret-access-key'                 = 'minioadmin',
    's3.path-style-access'                 = 'true',
    'client.region'                        = 'us-east-1'
);

CREATE TABLE IF NOT EXISTS polaris.bronze.meeza_digital_wallet_events (
    `_id`            STRING,
    wallet_id        STRING,
    event_type       STRING,
    channel          STRING,
    issuing_bank_id  STRING,
    amount_egp       DECIMAL(18,2),
    `status`         STRING,
    event_ts         TIMESTAMP(3),
    PRIMARY KEY (`_id`) NOT ENFORCED
) WITH (
    'format-version'                = '2',
    'write.upsert.enabled'          = 'true',
    'write.parquet.compression-codec' = 'snappy'
);

CREATE TEMPORARY TABLE kafka_wallet_src (
    `_id`            STRING,
    wallet_id        STRING,
    event_type       STRING,
    channel          STRING,
    issuing_bank_id  STRING,
    amount_egp       DECIMAL(18,2),
    `status`         STRING,
    event_ts         TIMESTAMP(3),
    PRIMARY KEY (`_id`) NOT ENFORCED,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.meeza_digital.wallet_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-iceberg-sink-digital_wallet',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO polaris.bronze.meeza_digital_wallet_events /*+ OPTIONS(
    'upsert-enabled'         = 'true',
    'equality-field-columns' = '_id'
) */
SELECT * FROM kafka_wallet_src;
