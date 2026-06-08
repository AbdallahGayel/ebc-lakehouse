-- =============================================================================
-- mssql/init/03_enable_cdc.sql
-- Enables SQL Server CDC on dbo.atm_sessions so Flink CDC can tail the log.
-- Idempotent: skips if already enabled.
-- =============================================================================
USE ebc_atm;
GO

IF NOT EXISTS (
    SELECT 1
    FROM   sys.tables t
           JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = N'dbo' AND t.name = N'atm_sessions' AND t.is_tracked_by_cdc = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema           = N'dbo',
        @source_name             = N'atm_sessions',
        @role_name               = NULL,            -- no gating role; service account reads directly
        @supports_net_changes    = 1,
        @capture_instance        = N'dbo_atm_sessions';
END
GO
