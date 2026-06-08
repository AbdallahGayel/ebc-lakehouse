-- =============================================================================
-- 01_register_polaris_catalog.sql
--
-- Register the Polaris-backed Iceberg catalog with the Flink SQL Gateway.
-- Every subsequent job references tables via `polaris.bronze.<table>`.
--
-- This statement is idempotent: CREATE CATALOG IF NOT EXISTS is supported
-- by Flink ≥ 1.18. The catalog config mirrors trino/etc/catalog/iceberg.
-- properties — same OAuth2 credential, same warehouse, same vended-creds
-- behaviour.
-- =============================================================================

-- NOTE: do NOT `USE CATALOG polaris` here. The submitter opens one SQL
-- Gateway session per file, but inside a single file session state
-- persists across statements. If we switched the active catalog here,
-- subsequent CREATE TEMPORARY TABLEs in CDC source files (10_*, 11_*,
-- 12_*) would register into `polaris.bronze.*` — where Iceberg's
-- validator rejects non-Iceberg `connector` options like `postgres-cdc`
-- and `sqlserver-cdc`. Keep the active catalog at default_catalog and
-- reference Iceberg tables fully-qualified (`polaris.bronze.<table>`)
-- in the 20-range sink jobs.

CREATE CATALOG IF NOT EXISTS polaris WITH (
    'type'                                 = 'iceberg',
    'catalog-impl'                         = 'org.apache.iceberg.rest.RESTCatalog',
    'uri'                                  = 'http://polaris:8181/api/catalog',
    'warehouse'                            = 'ebc_lakehouse',
    'credential'                           = 'root:s3cr3t',
    'scope'                                = 'PRINCIPAL_ROLE:ALL',
    'token-refresh-enabled'                = 'true',
    'header.X-Iceberg-Access-Delegation'   = 'vended-credentials',
    'io-impl'                              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'                          = 'http://minio:9000',
    's3.access-key-id'                     = 'minioadmin',
    's3.secret-access-key'                 = 'minioadmin',
    's3.path-style-access'                 = 'true',
    'client.region'                        = 'us-east-1'
);
