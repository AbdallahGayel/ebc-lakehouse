-- =============================================================================
-- 22_sink_ipn_transactions.sql
-- Kafka topic (raw CDC) → Iceberg bronze.ipn_transactions
-- Watermark on initiated_at (60 s budget — IPN events can land out-of-order
-- across schemes); upsert keyed on txn_id.
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

CREATE TABLE IF NOT EXISTS polaris.bronze.ipn_transactions (
    txn_id              STRING,
    payment_ref         STRING,
    sender_proxy        STRING,
    receiver_proxy      STRING,
    sender_bank_id      STRING,
    receiver_bank_id    STRING,
    amount_egp          DOUBLE,
    payment_purpose     STRING,
    txn_status          STRING,
    failure_reason      STRING,
    initiation_channel  STRING,
    processing_time_ms  BIGINT,
    initiated_at        TIMESTAMP(3),
    completed_at        TIMESTAMP(3),
    PRIMARY KEY (txn_id) NOT ENFORCED
) WITH (
    'format-version'                = '2',
    'write.upsert.enabled'          = 'true',
    'write.parquet.compression-codec' = 'snappy'
);

CREATE TEMPORARY TABLE kafka_ipn_src (
    txn_id              STRING,
    payment_ref         STRING,
    sender_proxy        STRING,
    receiver_proxy      STRING,
    sender_bank_id      STRING,
    receiver_bank_id    STRING,
    amount_egp          DOUBLE,
    payment_purpose     STRING,
    txn_status          STRING,
    failure_reason      STRING,
    initiation_channel  STRING,
    processing_time_ms  BIGINT,
    initiated_at        TIMESTAMP(3),
    completed_at        TIMESTAMP(3),
    PRIMARY KEY (txn_id) NOT ENFORCED,
    WATERMARK FOR initiated_at AS initiated_at - INTERVAL '60' SECOND
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.public.ipn_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-iceberg-sink-ipn_transactions',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO polaris.bronze.ipn_transactions /*+ OPTIONS(
    'upsert-enabled'         = 'true',
    'equality-field-columns' = 'txn_id'
) */
SELECT * FROM kafka_ipn_src;
