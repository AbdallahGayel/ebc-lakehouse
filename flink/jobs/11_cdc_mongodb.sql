-- =============================================================================
-- 11_cdc_mongodb.sql
--
-- Flink CDC source for the Meeza Digital wallet collection. Tails the rs0
-- replica-set oplog (no resume-token state-store needed in dev — Flink
-- checkpoints carry it).
--
-- Replaces:
--   kafka/connectors/debezium-digital-wallet.json
--
-- Before submitting:
--   curl -X DELETE http://localhost:8083/connectors/debezium-digital-wallet
-- =============================================================================

CREATE TEMPORARY TABLE mongo_wallet (
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
    'connector'           = 'mongodb-cdc',
    'hosts'               = 'mongodb:27017',
    'database'            = 'meeza_digital',
    'collection'          = 'wallet_events',
    'scan.startup.mode'   = 'initial',
    -- Dev replica-set has no auth; in prod set username/password.
    'connection.options'  = 'replicaSet=rs0'
);

CREATE TEMPORARY TABLE kafka_wallet_sink (
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
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ebc.meeza_digital.wallet_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format'                   = 'avro-confluent',
    'key.avro-confluent.url'       = 'http://schema-registry:8081',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

INSERT INTO kafka_wallet_sink SELECT * FROM mongo_wallet;
