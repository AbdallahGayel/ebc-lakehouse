-- =============================================================================
-- postgres/init/00_init_roles.sql
-- Pure SQL — no psql meta-commands. Docker executes this with: psql -f
-- Runs after 00_create_ebc_src_db.sh (alphabetical, .sh before .sql).
-- =============================================================================

-- 1. Create postgres superuser alias
--    Debezium JDBC probes fall back to user "postgres" when no user is given.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
        CREATE ROLE postgres SUPERUSER LOGIN PASSWORD 'postgres';
    END IF;
END $$;

-- 2. Grant REPLICATION + LOGIN to ebc_src
--    Must be set HERE, before scripts 01-04 run, so that
--    CREATE PUBLICATION in those scripts succeeds.
ALTER ROLE ebc_src REPLICATION LOGIN;

-- 3. Ensure ebc_src owns the source database and has full access
GRANT ALL PRIVILEGES ON DATABASE ebc_sources TO ebc_src;
