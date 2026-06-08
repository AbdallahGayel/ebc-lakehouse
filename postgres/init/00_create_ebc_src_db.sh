#!/usr/bin/env bash
# =============================================================================
# postgres/init/00_create_ebc_src_db.sh
# Creates the ebc_src convenience database.
# Must NOT use set -e — Docker initdb stops all further scripts on any exit != 0.
# =============================================================================

echo "[initdb] Creating convenience database 'ebc_src' if it does not exist..."

DB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT 1 FROM pg_database WHERE datname='ebc_src'")

if [ "$DB_EXISTS" = "1" ]; then
    echo "[initdb] Database 'ebc_src' already exists — skipping."
else
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "CREATE DATABASE ebc_src OWNER $POSTGRES_USER;"
    echo "[initdb] Database 'ebc_src' created."
fi
