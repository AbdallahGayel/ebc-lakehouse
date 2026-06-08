-- DataHub MySQL Schema — EBC Lakehouse
-- Compatible with acryldata/datahub-gms:v1.5.0.3
-- Collation: utf8mb4_bin (binary, case-sensitive — required for URN key lookups)
--
-- This file is mounted at /docker-entrypoint-initdb.d/init.sql and runs once
-- on the first MySQL container start (empty volume).  On re-runs with stale
-- data, ensure_datahub_mysql_clean() in start.sh sources this file directly.

CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
  urn            VARCHAR(500)  NOT NULL,
  aspect         VARCHAR(200)  NOT NULL,
  version        BIGINT(20)    NOT NULL,
  metadata       LONGTEXT      NOT NULL,
  systemmetadata LONGTEXT,
  createdon      DATETIME(6)   NOT NULL,
  createdby      VARCHAR(255)  NOT NULL,
  createdfor     VARCHAR(255),
  CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn, aspect, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
