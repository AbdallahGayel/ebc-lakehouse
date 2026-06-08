-- =============================================================================
-- 12_cdc_sqlserver.sql
-- Flink CDC source for MS SQL Server's atm_sessions table. Tails the CDC
-- capture instance `dbo_atm_sessions` (enabled by mssql/init/03_enable_cdc.sql)
-- and publishes Debezium-equivalent change events to the Bronze Kafka topic
-- `ebc.dbo.atm_sessions`.
--
-- Replaces the legacy `cassandra-atm-sessions` topic that the Kafka Connect
-- Debezium Cassandra source used to produce — see also the rename of the
-- Iceberg Bronze table from `cassandra_atm_sessions` to `atm_sessions`.
-- =============================================================================

CREATE TEMPORARY TABLE mssql_atm (
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
    'connector'       = 'sqlserver-cdc',
    'hostname'        = 'mssql',
    'port'            = '1433',
    'username'        = 'sa',
    'password'        = 'EbcAtm_S3cret!',
    'database-name'   = 'ebc_atm',
    -- sqlserver-cdc 3.2.0 doesn't accept `schema-name` — schema goes in `table-name`.
    'table-name'      = 'dbo.atm_sessions',
    -- The dev mssql-server-2022 image ships a self-signed cert. Disable
    -- encrypt-required + trust-cert via the Debezium passthrough prefix
    -- (sqlserver-cdc doesn't expose jdbc.properties.* directly).
    'debezium.database.encrypt'              = 'false',
    'debezium.database.trustServerCertificate' = 'true',
    'scan.incremental.snapshot.enabled'   = 'true'
);

CREATE TEMPORARY TABLE kafka_atm_sink (
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
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.dbo.atm_sessions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO kafka_atm_sink SELECT * FROM mssql_atm;
