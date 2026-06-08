-- =============================================================================
-- mssql/init/02_create_atm_sessions.sql
-- Mirrors the Cassandra `atm_sessions` schema in T-SQL with a single primary
-- table — Cassandra's denormalised access-pattern tables (by_card, by_ingest)
-- are no longer needed because:
--   • Flink CDC reads the transaction log (no scans to optimise away).
--   • Downstream queries land in Iceberg via Bronze, where Trino does the
--     equivalent of the by_card / by_ingest queries with normal indexed
--     reads off Parquet partitions.
-- =============================================================================
USE ebc_atm;
GO

IF OBJECT_ID(N'dbo.atm_sessions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.atm_sessions (
        atm_id            VARCHAR(32)     NOT NULL,
        session_date      DATE            NOT NULL,
        session_id        UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        session_ts        DATETIME2(3)    NOT NULL,
        card_token        VARCHAR(64)     NULL,
        issuing_bank_id   VARCHAR(16)     NULL,
        txn_type          VARCHAR(32)     NOT NULL,
        amount_egp        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        currency_code     VARCHAR(3)      NOT NULL DEFAULT 'EGP',
        response_code     VARCHAR(8)      NULL,
        [status]          VARCHAR(16)     NOT NULL,
        atm_location      VARCHAR(128)    NULL,
        governorate       VARCHAR(64)     NULL,
        network_id        VARCHAR(16)     NULL,
        processing_ms     INT             NULL,
        error_code        VARCHAR(64)     NULL,
        created_at        DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at        DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_atm_sessions PRIMARY KEY CLUSTERED (atm_id, session_date, session_id)
    );

    -- Secondary index for fraud/dispute lookups (the old by_card table use-case).
    CREATE NONCLUSTERED INDEX IX_atm_sessions_card_date
        ON dbo.atm_sessions (card_token, session_date)
        INCLUDE (atm_id, txn_type, amount_egp, [status], updated_at);
END
GO
