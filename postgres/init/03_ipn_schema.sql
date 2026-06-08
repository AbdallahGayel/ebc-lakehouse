-- =============================================================================
-- postgres/init/03_ipn_schema.sql
-- IPN: InstaPay / Instant Payment Network — real-time P2P and merchant payments
-- Logical replication (WAL) configured for Debezium CDC
-- =============================================================================

CREATE TABLE IF NOT EXISTS ipn_transactions (
    txn_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_ref         VARCHAR(30)  UNIQUE NOT NULL,
    sender_proxy        VARCHAR(100) NOT NULL,           -- Mobile/IBAN/Alias (hashed)
    receiver_proxy      VARCHAR(100) NOT NULL,
    sender_bank_id      VARCHAR(10)  NOT NULL,
    receiver_bank_id    VARCHAR(10)  NOT NULL,
    amount_egp          NUMERIC(18,2) NOT NULL,
    payment_purpose     VARCHAR(50),                     -- P2P/MERCHANT/BILL/SALARY
    txn_status          VARCHAR(20)  NOT NULL DEFAULT 'PROCESSING',
    failure_reason      VARCHAR(100),
    initiation_channel  VARCHAR(30),                     -- INSTAPAY_APP/BANK_APP/USSD
    processing_time_ms  INTEGER,                         -- End-to-end latency in ms
    initiated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Required by Debezium: full row image on UPDATE/DELETE
ALTER TABLE ipn_transactions REPLICA IDENTITY FULL;

-- Per-table publication for the dedicated Debezium IPN connector
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_ipn_pub') THEN
        EXECUTE 'CREATE PUBLICATION debezium_ipn_pub FOR TABLE ipn_transactions';
        RAISE NOTICE 'Created publication: debezium_ipn_pub';
    ELSE
        RAISE NOTICE 'Publication debezium_ipn_pub already exists';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ipn_initiated   ON ipn_transactions(initiated_at);
CREATE INDEX IF NOT EXISTS idx_ipn_status      ON ipn_transactions(txn_status);
CREATE INDEX IF NOT EXISTS idx_ipn_sender_bank ON ipn_transactions(sender_bank_id);

-- Seed 3 000 sample IPN transactions
INSERT INTO ipn_transactions
    (payment_ref, sender_proxy, receiver_proxy,
     sender_bank_id, receiver_bank_id, amount_egp, payment_purpose,
     txn_status, processing_time_ms, initiation_channel, initiated_at, completed_at)
SELECT
    'IPN' || TO_CHAR(NOW(), 'YYYYMMDD') || LPAD(n::text, 10, '0'),
    md5(random()::text),
    md5(random()::text),
    (ARRAY['CIB','NBE','QNB','BDC','MIB','HSBC','FAB','AGB'])[floor(random()*8+1)],
    (ARRAY['CIB','NBE','QNB','BDC','MIB','HSBC','FAB','AGB'])[floor(random()*8+1)],
    ROUND((random()*9900+100)::numeric, 2),
    (ARRAY['P2P','P2P','MERCHANT','BILL','SALARY'])[floor(random()*5+1)],
    (ARRAY['COMPLETED','COMPLETED','COMPLETED','FAILED','REVERSED'])[floor(random()*5+1)],
    (random()*1800+200)::int,
    (ARRAY['INSTAPAY_APP','BANK_APP','BANK_APP','USSD'])[floor(random()*4+1)],
    NOW() - ((random()*3)::int || ' days')::interval
         - ((random()*86400)::int || ' seconds')::interval,
    NOW() - ((random()*3)::int || ' days')::interval
FROM generate_series(1, 3000) n;

