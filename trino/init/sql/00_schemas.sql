-- =============================================================================
-- 00_schemas.sql
-- Create the four medallion schemas in the Trino `iceberg` catalog. The
-- Polaris namespace bootstrap (polaris/catalog-init/init_polaris.py) creates
-- the same namespaces from the catalog side; running this from Trino too is
-- redundant but harmless and keeps Trino itself fully self-bootstrappable.
--
-- Trino-side `CREATE SCHEMA` against an Iceberg REST catalog is forwarded
-- as a `POST /v1/namespaces` to Polaris. The catalog's
-- default-base-location (s3://ebc-lakehouse/) means each schema lands at
-- s3://ebc-lakehouse/<schema>/.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3://ebc-lakehouse/bronze/');

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3://ebc-lakehouse/silver/');

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3://ebc-lakehouse/gold/');

CREATE SCHEMA IF NOT EXISTS iceberg.serving
WITH (location = 's3://ebc-lakehouse/serving/');
