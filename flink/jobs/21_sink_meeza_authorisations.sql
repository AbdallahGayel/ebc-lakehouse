-- =============================================================================
-- 21_sink_meeza_authorisations.sql
-- Kafka topic (raw CDC) → Iceberg bronze.meeza_authorisations
-- Watermark on auth_timestamp (20 s out-of-order budget); upsert keyed on auth_id.
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

CREATE TABLE IF NOT EXISTS polaris.bronze.meeza_authorisations (
    auth_id            STRING,
    card_token         STRING,
    merchant_id        STRING,
    merchant_category  STRING,
    terminal_id        STRING,
    txn_type           STRING,
    channel            STRING,
    amount_egp         DECIMAL(18,2),
    currency_code      STRING,
    response_code      STRING,
    auth_status        STRING,
    issuing_bank_id    STRING,
    acquiring_bank_id  STRING,
    is_international   BOOLEAN,
    auth_timestamp     TIMESTAMP(3),
    settlement_date    DATE,
    PRIMARY KEY (auth_id) NOT ENFORCED
) WITH (
    'format-version'                = '2',
    'write.upsert.enabled'          = 'true',
    'write.parquet.compression-codec' = 'snappy'
);

CREATE TEMPORARY TABLE kafka_meeza_src (
    auth_id            STRING,
    card_token         STRING,
    merchant_id        STRING,
    merchant_category  STRING,
    terminal_id        STRING,
    txn_type           STRING,
    channel            STRING,
    amount_egp         DECIMAL(18,2),
    currency_code      STRING,
    response_code      STRING,
    auth_status        STRING,
    issuing_bank_id    STRING,
    acquiring_bank_id  STRING,
    is_international   BOOLEAN,
    auth_timestamp     TIMESTAMP(3),
    settlement_date    DATE,
    PRIMARY KEY (auth_id) NOT ENFORCED,
    WATERMARK FOR auth_timestamp AS auth_timestamp - INTERVAL '20' SECOND
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.public.meeza_authorisations',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-iceberg-sink-meeza_authorisations',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO polaris.bronze.meeza_authorisations /*+ OPTIONS(
    'upsert-enabled'         = 'true',
    'equality-field-columns' = 'auth_id'
) */
SELECT * FROM kafka_meeza_src;
