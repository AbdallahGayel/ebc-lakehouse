-- =============================================================================
-- postgres/init/02_meeza_schema.sql
-- Meeza National Card Scheme: POS, e-commerce, ATM authorisations
-- Logical replication (WAL) configured for Debezium CDC
-- =============================================================================

CREATE TABLE IF NOT EXISTS meeza_authorisations (
    auth_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    card_token          VARCHAR(64)  NOT NULL,           -- Tokenised PAN (no raw card data)
    merchant_id         VARCHAR(15),
    merchant_category   VARCHAR(50),                     -- MCC description
    terminal_id         VARCHAR(8),                      -- POS terminal or ATM ID
    txn_type            VARCHAR(20)  NOT NULL,           -- PURCHASE/REFUND/WITHDRAWAL/INQUIRY
    channel             VARCHAR(20)  NOT NULL,           -- POS/ECOMMERCE/ATM/CONTACTLESS
    amount_egp          NUMERIC(18,2),
    currency_code       CHAR(3)      DEFAULT 'EGP',
    auth_code           VARCHAR(6),
    response_code       VARCHAR(3)   NOT NULL,           -- ISO 8583: 00=approved
    auth_status         VARCHAR(20)  NOT NULL,           -- APPROVED/DECLINED/REVERSED
    issuing_bank_id     VARCHAR(10)  NOT NULL,
    acquiring_bank_id   VARCHAR(10),
    is_international    BOOLEAN      DEFAULT FALSE,
    auth_timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    settlement_date     DATE,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Required by Debezium: full row image on UPDATE/DELETE
ALTER TABLE meeza_authorisations REPLICA IDENTITY FULL;

-- Per-table publication for the dedicated Debezium Meeza connector
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_meeza_pub') THEN
        EXECUTE 'CREATE PUBLICATION debezium_meeza_pub FOR TABLE meeza_authorisations';
        RAISE NOTICE 'Created publication: debezium_meeza_pub';
    ELSE
        RAISE NOTICE 'Publication debezium_meeza_pub already exists';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_meeza_auth_ts  ON meeza_authorisations(auth_timestamp);
CREATE INDEX IF NOT EXISTS idx_meeza_status   ON meeza_authorisations(auth_status);
CREATE INDEX IF NOT EXISTS idx_meeza_channel  ON meeza_authorisations(channel);

-- Seed 2 000 sample Meeza card authorisations
INSERT INTO meeza_authorisations
    (card_token, merchant_id, merchant_category, terminal_id,
     txn_type, channel, amount_egp, auth_code, response_code, auth_status,
     issuing_bank_id, acquiring_bank_id, is_international, auth_timestamp, settlement_date)
SELECT
    md5(random()::text),
    'MER' || LPAD((random()*99999)::int::text, 8, '0'),
    (ARRAY['Grocery','Fuel','Restaurant','Pharmacy','Retail','Utility','Government','Telecom'])[floor(random()*8+1)],
    'T' || LPAD((random()*9999999)::int::text, 7, '0'),
    (ARRAY['PURCHASE','PURCHASE','PURCHASE','WITHDRAWAL','REFUND'])[floor(random()*5+1)],
    (ARRAY['POS','POS','ECOMMERCE','ATM','CONTACTLESS'])[floor(random()*5+1)],
    ROUND((random()*4900+100)::numeric, 2),
    LPAD((random()*999999)::int::text, 6, '0'),
    (ARRAY['00','00','00','05','51','14'])[floor(random()*6+1)],
    (ARRAY['APPROVED','APPROVED','APPROVED','DECLINED','REVERSED'])[floor(random()*5+1)],
    (ARRAY['CIB','NBE','QNB','BDC','MIB'])[floor(random()*5+1)],
    (ARRAY['CIB','NBE','QNB','NAPS','EBC'])[floor(random()*5+1)],
    random() < 0.03,
    NOW() - ((random()*7)::int || ' days')::interval
         - ((random()*86400)::int || ' seconds')::interval,
    CURRENT_DATE - (random()*7)::int
FROM generate_series(1, 2000);

