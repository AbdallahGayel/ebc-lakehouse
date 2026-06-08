-- =============================================================================
-- postgres/init/01_ach_schema.sql
-- EG-ACH: Automated Clearing House — bank-to-bank transfers
-- Logical replication (WAL) configured for Debezium CDC
-- =============================================================================

CREATE TABLE IF NOT EXISTS ach_transactions (
    txn_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            VARCHAR(50)  NOT NULL,
    txn_type            VARCHAR(20)  NOT NULL,           -- CREDIT / DEBIT / RETURN
    originating_bank_id VARCHAR(10)  NOT NULL,           -- CBE bank code
    receiving_bank_id   VARCHAR(10)  NOT NULL,
    originator_account  VARCHAR(24)  NOT NULL,           -- IBAN-format
    receiver_account    VARCHAR(24)  NOT NULL,
    amount_egp          NUMERIC(18,2) NOT NULL,
    currency_code       CHAR(3)      DEFAULT 'EGP',
    txn_status          VARCHAR(20)  DEFAULT 'PENDING',  -- PENDING/SETTLED/RETURNED/FAILED
    settlement_date     DATE         NOT NULL,
    effective_date      DATE         NOT NULL,
    description         VARCHAR(140),
    error_code          VARCHAR(10),
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Required by Debezium: full row image on UPDATE/DELETE (not just changed columns)
ALTER TABLE ach_transactions REPLICA IDENTITY FULL;

-- Per-table publication for the dedicated Debezium ACH connector
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_ach_pub') THEN
        EXECUTE 'CREATE PUBLICATION debezium_ach_pub FOR TABLE ach_transactions';
        RAISE NOTICE 'Created publication: debezium_ach_pub';
    ELSE
        RAISE NOTICE 'Publication debezium_ach_pub already exists';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ach_settlement_date ON ach_transactions(settlement_date);
CREATE INDEX IF NOT EXISTS idx_ach_status          ON ach_transactions(txn_status);
CREATE INDEX IF NOT EXISTS idx_ach_created_at      ON ach_transactions(created_at);

-- Seed 1 000 sample ACH transactions
INSERT INTO ach_transactions
    (batch_id, txn_type, originating_bank_id, receiving_bank_id,
     originator_account, receiver_account, amount_egp, txn_status,
     settlement_date, effective_date, description)
SELECT
    'BATCH-' || LPAD((random()*9999)::int::text, 6, '0'),
    (ARRAY['CREDIT','DEBIT','CREDIT','CREDIT'])[floor(random()*4+1)],
    (ARRAY['CIB','NBE','QNB','BDC','MIB','HSBC','FAB','AGB'])[floor(random()*8+1)],
    (ARRAY['CIB','NBE','QNB','BDC','MIB','HSBC','FAB','AGB'])[floor(random()*8+1)],
    'EG' || LPAD((random()*999999999999)::bigint::text, 22, '0'),
    'EG' || LPAD((random()*999999999999)::bigint::text, 22, '0'),
    ROUND((random()*99000+1000)::numeric, 2),
    (ARRAY['SETTLED','SETTLED','SETTLED','PENDING','FAILED'])[floor(random()*5+1)],
    CURRENT_DATE - (random()*30)::int,
    CURRENT_DATE - (random()*30)::int,
    (ARRAY['Salary Payment','Bill Payment','Transfer','Direct Debit','Vendor Payment'])[floor(random()*5+1)]
FROM generate_series(1, 1000);

