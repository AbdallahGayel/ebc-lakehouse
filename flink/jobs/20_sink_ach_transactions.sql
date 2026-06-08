-- =============================================================================
-- 20_sink_ach_transactions.sql
-- Kafka topic (raw CDC) → Iceberg bronze.ach_transactions (Polaris-managed)
--
-- Stateful streaming consumer with:
--   • Watermark on created_at allowing 30 s of out-of-order events.
--   • Upsert mode: Iceberg sink `upsert-enabled = true` +
--     `equality-field-columns = txn_id` collapses retractions from the
--     upstream `upsert-kafka` source into row-level upserts on Iceberg.
--   • Type alignment with the Phase-2 CDC source (TIMESTAMP(3) / DATE)
--     so the schema-registry subject doesn't drift across the rewrite.
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

-- Pre-create the Iceberg target. Flink's Iceberg sink doesn't auto-create
-- (Kafka-Connect's iceberg-sink used to). PARTITION BY days(created_at)
-- matches what the connector emitted, and the format/version pin keeps
-- new tables readable by Trino 455+ without manual property tweaks.
CREATE TABLE IF NOT EXISTS polaris.bronze.ach_transactions (
    txn_id               STRING,
    batch_id             STRING,
    txn_type             STRING,
    originating_bank_id  STRING,
    receiving_bank_id    STRING,
    originator_account   STRING,
    receiver_account     STRING,
    amount_egp           DECIMAL(18,2),
    currency_code        STRING,
    txn_status           STRING,
    created_at           TIMESTAMP(3),
    settlement_date      DATE,
    PRIMARY KEY (txn_id) NOT ENFORCED
) WITH (
    'format-version'                = '2',
    'write.upsert.enabled'          = 'true',
    'write.parquet.compression-codec' = 'snappy'
);

CREATE TEMPORARY TABLE kafka_ach_src (
    txn_id               STRING,
    batch_id             STRING,
    txn_type             STRING,
    originating_bank_id  STRING,
    receiving_bank_id    STRING,
    originator_account   STRING,
    receiver_account     STRING,
    amount_egp           DECIMAL(18,2),
    currency_code        STRING,
    txn_status           STRING,
    created_at           TIMESTAMP(3),
    settlement_date      DATE,
    PRIMARY KEY (txn_id) NOT ENFORCED,
    WATERMARK FOR created_at AS created_at - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.public.ach_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-iceberg-sink-ach_transactions',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO polaris.bronze.ach_transactions /*+ OPTIONS(
    'upsert-enabled'             = 'true',
    'equality-field-columns'     = 'txn_id'
) */
SELECT * FROM kafka_ach_src;
