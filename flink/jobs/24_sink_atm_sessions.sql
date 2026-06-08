-- =============================================================================
-- 24_sink_atm_sessions.sql
-- Kafka topic ebc.dbo.atm_sessions (from Flink CDC sqlserver-cdc) →
-- Iceberg bronze.atm_sessions (Polaris-managed).
--
-- Renamed from the old Cassandra path: the table used to be
-- `bronze.cassandra_atm_sessions` fed from `cassandra-atm-sessions`. Both
-- the topic and the Iceberg table now drop the `cassandra_` prefix — the
-- source is SQL Server.
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

CREATE TABLE IF NOT EXISTS polaris.bronze.atm_sessions (
    atm_id            STRING,
    session_date      DATE,
    session_id        STRING,
    session_ts        TIMESTAMP(3),
    card_token        STRING,
    issuing_bank_id   STRING,
    txn_type          STRING,
    amount_egp        DECIMAL(18,2),
    currency_code     STRING,
    response_code     STRING,
    `status`          STRING,
    atm_location      STRING,
    governorate       STRING,
    network_id        STRING,
    processing_ms     INT,
    error_code        STRING,
    created_at        TIMESTAMP(3),
    updated_at        TIMESTAMP(3),
    PRIMARY KEY (atm_id, session_date, session_id) NOT ENFORCED
) WITH (
    'format-version'                = '2',
    'write.upsert.enabled'          = 'true',
    'write.parquet.compression-codec' = 'snappy'
);

CREATE TEMPORARY TABLE kafka_atm_src (
    atm_id            STRING,
    session_date      DATE,
    session_id        STRING,
    session_ts        TIMESTAMP(3),
    card_token        STRING,
    issuing_bank_id   STRING,
    txn_type          STRING,
    amount_egp        DECIMAL(18,2),
    currency_code     STRING,
    response_code     STRING,
    `status`          STRING,
    atm_location      STRING,
    governorate       STRING,
    network_id        STRING,
    processing_ms     INT,
    error_code        STRING,
    created_at        TIMESTAMP(3),
    updated_at        TIMESTAMP(3),
    PRIMARY KEY (atm_id, session_date, session_id) NOT ENFORCED,
    -- ATM telemetry can lag by minutes (embedded clocks, batch upload windows);
    -- keep the 5-min budget that the Cassandra-era job had.
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' MINUTE
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.dbo.atm_sessions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-iceberg-sink-atm_sessions',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO polaris.bronze.atm_sessions /*+ OPTIONS(
    'upsert-enabled'         = 'true',
    'equality-field-columns' = 'atm_id,session_date,session_id'
) */
SELECT * FROM kafka_atm_src;
