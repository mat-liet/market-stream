#!/usr/bin/env bash
#
# Applies ClickHouse migrations on every `make up`.
#
# Flyway has no dependable ClickHouse support, so migrations here are plain SQL
# files applied in filename order. Every statement must be idempotent
# (CREATE ... IF NOT EXISTS, ALTER TABLE ... ADD COLUMN IF NOT EXISTS) because
# this script re-runs against an already-migrated database on each startup.

set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-market}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-market}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-/migrations}"

client() {
  clickhouse-client \
    --host "${CLICKHOUSE_HOST}" \
    --user "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    "$@"
}

echo "Waiting for ClickHouse at ${CLICKHOUSE_HOST} ..."
until client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "Applying migrations from ${MIGRATIONS_DIR}:"
for file in "${MIGRATIONS_DIR}"/*.sql; do
  echo "  $(basename "${file}")"
  client --multiquery --queries-file "${file}"
done

echo
echo "Tables in market:"
client --query "SHOW TABLES FROM market" | sed 's/^/  /'
echo
echo "Done."
