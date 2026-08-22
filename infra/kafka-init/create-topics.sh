#!/usr/bin/env bash
#
# Creates the full topic catalogue from section 11.2 of the design doc.
#
# Topics are created explicitly rather than by broker auto-create, because
# auto-create uses the wrong partition count, retention and cleanup policy —
# and partition count cannot be reduced later without breaking key->partition
# stability, and therefore replay determinism (design doc 11.3).
#
# Phase 1 only uses raw.kraken.trade, normalized.trades, derived.candles and
# the two ops topics, but the whole catalogue is created now: it costs nothing
# and makes phase 3 a code-only change.

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

DAY_MS=86400000

create_topic() {
  local name="$1" partitions="$2" cleanup="$3" retention_days="$4"

  local -a config=(--config "cleanup.policy=${cleanup}")
  if [[ "${cleanup}" == "delete" ]]; then
    config+=(--config "retention.ms=$((retention_days * DAY_MS))")
  else
    # Compacted topics: keep tombstones long enough for a slow consumer to see them.
    config+=(--config "min.cleanable.dirty.ratio=0.1" --config "delete.retention.ms=$((DAY_MS))")
  fi

  kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    "${config[@]}" >/dev/null

  printf '  %-28s partitions=%-2s cleanup=%-7s retention=%s\n' \
    "${name}" "${partitions}" "${cleanup}" \
    "$([[ "${cleanup}" == "delete" ]] && echo "${retention_days}d" || echo "-")"
}

echo "Waiting for Kafka at ${BOOTSTRAP_SERVER} ..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP_SERVER}" >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topics:"

#            name                        parts  cleanup  retention(d)
# --- raw: the immutable replay source of truth, longest affordable retention
create_topic raw.kraken.trade              6    delete   7
create_topic raw.kraken.book               6    delete   7

# --- normalized: cheap canonical intermediate, always rederivable from raw
create_topic normalized.trades             6    delete   3
create_topic normalized.book.events        6    delete   3

# --- derived: the product; ClickHouse becomes the query surface beyond this
create_topic derived.candles               6    delete   14
create_topic derived.book.metrics          6    delete   3
create_topic alerts                        6    delete   30

# --- state: a table of latest-per-key, so compacted rather than time-retained
create_topic state.book.current            6    compact  0

# --- ops: structured data-quality failures vs unparseable frames
create_topic invalid.events                3    delete   14
create_topic dead-letter                   3    delete   14

# --- control: processor -> ingestor resubscribe requests (phase 3)
create_topic control.book.resync           3    delete   1

echo
echo "Topic catalogue:"
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list | sort | sed 's/^/  /'
echo
echo "Done."
