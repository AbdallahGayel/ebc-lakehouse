-- =============================================================================
-- postgres/init/04_combined_publication.sql
-- Runs AFTER all three table schemas are created (alphabetical: 04 > 03).
--
-- Creates:
--   • A single combined publication covering all three tables (optional fallback)
--   • Verifies all per-table publications were created by the schema scripts
--   • Grants SELECT on all tables to ebc_src so the snapshot can read them
-- =============================================================================

-- ── Grant SELECT on all source tables to ebc_src ──────────────────────────────
-- Debezium snapshot mode reads all existing rows via SELECT before streaming WAL.
GRANT SELECT ON TABLE ach_transactions     TO ebc_src;
GRANT SELECT ON TABLE meeza_authorisations TO ebc_src;
GRANT SELECT ON TABLE ipn_transactions     TO ebc_src;

-- Also grant USAGE on the public schema
GRANT USAGE ON SCHEMA public TO ebc_src;

-- ── Combined publication (all three tables, one slot) ─────────────────────────
-- Useful if you ever want to run a single Debezium connector for all tables.
-- The per-connector configs in kafka/connectors/ use the individual publications.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_all_pub') THEN
        EXECUTE 'CREATE PUBLICATION debezium_all_pub FOR TABLE '
             || 'ach_transactions, meeza_authorisations, ipn_transactions';
        RAISE NOTICE 'Created publication: debezium_all_pub';
    ELSE
        RAISE NOTICE 'Publication debezium_all_pub already exists — skipping';
    END IF;
END $$;

-- ── Verify all per-table publications exist ───────────────────────────────────
DO $$
DECLARE
    missing TEXT := '';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_ach_pub') THEN
        missing := missing || ' debezium_ach_pub';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_meeza_pub') THEN
        missing := missing || ' debezium_meeza_pub';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_ipn_pub') THEN
        missing := missing || ' debezium_ipn_pub';
    END IF;

    IF missing <> '' THEN
        RAISE WARNING 'Missing publications:%  — check schema init scripts', missing;
    ELSE
        RAISE NOTICE 'All per-table publications verified OK';
    END IF;
END $$;

-- ── Summary query — visible in docker logs ────────────────────────────────────
SELECT
    pubname            AS publication,
    puballtables       AS all_tables,
    pubinsert          AS ins,
    pubupdate          AS upd,
    pubdelete          AS del
FROM pg_publication
ORDER BY pubname;

SELECT
    schemaname,
    tablename,
    rowsecurity
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
