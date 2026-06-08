# trino/etc/RULES_README.md

Documentation for the Trino file-based access-control rules defined in
`rules.json` (sibling file). The JSON itself must stay free of comments —
Trino's Jackson reader rejects unknown fields like `_comment`.

## Roles

Trino identifies callers via the `X-Trino-User` header (auth=none in dev).
Map your service to the right role by setting `TRINO_USER` accordingly.

| Trino user           | Purpose                                                                  |
|----------------------|--------------------------------------------------------------------------|
| `ebc_admin` / `root` | Full control on every catalog/schema/table. Use for one-off ops.         |
| `ebc_engineer`       | Writer role for dbt-trino + Airflow MERGE operators.                     |
|                      | READ: every layer. WRITE: silver / gold / serving.                       |
|                      | Bronze is read-only — only Kafka Connect writes Bronze.                  |
| `ebc_bi`             | BI / Superset / analyst role.                                            |
|                      | READ-ONLY on gold + serving + information_schema.                        |
|                      | Hidden from bronze (raw CDC) and silver (engineering surface).           |

## Adding a new role

1. Append an entry under `catalogs[]` granting catalog visibility.
2. Append an entry under `schemas[]` if the role should `own` (DDL) a schema.
3. Append an entry under `tables[]` with the per-table privileges
   (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `OWNERSHIP`, `GRANT_SELECT`).
4. (Optional) Append an entry under `queries[]` if the role should
   `execute` / `view` / `kill` queries.

Rules are evaluated top-to-bottom — **first match wins**. Place narrower
rules above broader ones.

Trino reloads `rules.json` every 30 s (`security.refresh-period=30s` in
`access-control.properties`) — no restart needed for policy edits.
