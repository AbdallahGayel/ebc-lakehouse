-- =============================================================================
-- 10_cdc_postgres.sql
--
-- Flink CDC sources for all three Postgres tables in postgres-src. Each
-- declares its own `postgres-cdc` source (Debezium engine embedded inside
-- Flink), then publishes to the existing Kafka topic via `upsert-kafka`
-- with the same `debezium-avro-confluent` envelope Debezium-on-Connect
-- used to emit. Downstream consumers (the 20-range sink jobs, DataHub
-- Kafka ingestion, the OL bridge) see no schema change.
--
-- Replaces:
--   kafka/connectors/debezium-ach.json
--   kafka/connectors/debezium-meeza.json
--   kafka/connectors/debezium-ipn.json
--
-- Before submitting this job:
--   1. DELETE the corresponding Debezium connectors:
--        curl -X DELETE http://localhost:8083/connectors/debezium-ach-connector
--        curl -X DELETE http://localhost:8083/connectors/debezium-meeza-connector
--        curl -X DELETE http://localhost:8083/connectors/debezium-ipn-connector
--   2. Drop their replication slots so Flink CDC can claim its own:
--        docker exec ebc-postgres-src psql -U ebc_src -d ebc_sources -c \
--          "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
--           WHERE slot_name LIKE 'debezium_%';"
--
-- All this is automated by scripts/promote_flink_cdc.sh.
-- =============================================================================

-- ── ach_transactions ────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE pg_ach (
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
    'connector'             = 'postgres-cdc',
    'hostname'              = 'postgres-src',
    'port'                  = '5432',
    'username'              = 'ebc_src',
    'password'              = 'ebc_src_pass',
    'database-name'         = 'ebc_sources',
    'schema-name'           = 'public',
    'table-name'            = 'ach_transactions',
    'slot.name'             = 'flink_cdc_ach',
    'decoding.plugin.name'  = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true'
);

CREATE TEMPORARY TABLE kafka_ach_sink (
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
    'connector'                          = 'upsert-kafka',
    'topic'                              = 'ebc.public.ach_transactions',
    'properties.bootstrap.servers'       = 'kafka:29092',
    'key.format'                         = 'avro-confluent',
    'key.avro-confluent.url'             = 'http://schema-registry:8081',
    'value.format'                       = 'avro-confluent',
    'value.avro-confluent.url'           = 'http://schema-registry:8081'
);

INSERT INTO kafka_ach_sink SELECT * FROM pg_ach;

-- ── meeza_authorisations ────────────────────────────────────────────────────
CREATE TEMPORARY TABLE pg_meeza (
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
    'connector'             = 'postgres-cdc',
    'hostname'              = 'postgres-src',
    'port'                  = '5432',
    'username'              = 'ebc_src',
    'password'              = 'ebc_src_pass',
    'database-name'         = 'ebc_sources',
    'schema-name'           = 'public',
    'table-name'            = 'meeza_authorisations',
    'slot.name'             = 'flink_cdc_meeza',
    'decoding.plugin.name'  = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true'
);

CREATE TEMPORARY TABLE kafka_meeza_sink (
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
    'connector'                          = 'upsert-kafka',
    'topic'                              = 'ebc.public.meeza_authorisations',
    'properties.bootstrap.servers'       = 'kafka:29092',
    'key.format'                         = 'avro-confluent',
    'key.avro-confluent.url'             = 'http://schema-registry:8081',
    'value.format'                       = 'avro-confluent',
    'value.avro-confluent.url'           = 'http://schema-registry:8081'
);

INSERT INTO kafka_meeza_sink SELECT * FROM pg_meeza;

-- ── ipn_transactions ────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE pg_ipn (
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
    'connector'             = 'postgres-cdc',
    'hostname'              = 'postgres-src',
    'port'                  = '5432',
    'username'              = 'ebc_src',
    'password'              = 'ebc_src_pass',
    'database-name'         = 'ebc_sources',
    'schema-name'           = 'public',
    'table-name'            = 'ipn_transactions',
    'slot.name'             = 'flink_cdc_ipn',
    'decoding.plugin.name'  = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true'
);

CREATE TEMPORARY TABLE kafka_ipn_sink (
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
    'connector'                          = 'upsert-kafka',
    'topic'                              = 'ebc.public.ipn_transactions',
    'properties.bootstrap.servers'       = 'kafka:29092',
    'key.format'                         = 'avro-confluent',
    'key.avro-confluent.url'             = 'http://schema-registry:8081',
    'value.format'                       = 'avro-confluent',
    'value.avro-confluent.url'           = 'http://schema-registry:8081'
);

INSERT INTO kafka_ipn_sink SELECT * FROM pg_ipn;
