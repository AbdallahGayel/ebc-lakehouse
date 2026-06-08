-- =============================================================================
-- mssql/init/01_create_database.sql
-- Creates the ebc_atm database (replaces the Cassandra atm_network keyspace).
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'ebc_atm')
BEGIN
    CREATE DATABASE ebc_atm
        COLLATE Latin1_General_100_CI_AS_SC_UTF8;
END
GO

USE ebc_atm;
GO

-- Required for SQL Server CDC: enable at the database level. The default
-- capture instance + cleanup jobs run under SQL Server Agent, which the
-- mssql/server image starts automatically.
IF NOT EXISTS (SELECT 1 FROM sys.databases
               WHERE name = N'ebc_atm' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO
