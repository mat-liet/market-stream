-- Config store schema (design doc 15.3).
--
-- Postgres holds the instrument registry, the processor/schema version registry
-- and replay-job bookkeeping: small, relational, transactional data that has no
-- business being in Kafka.
--
-- These files are mounted into docker-entrypoint-initdb.d for phase 1, and are
-- named with Flyway's V__ convention so they can move under Flyway unchanged
-- once a service owns the migration lifecycle.

-- Which markets to subscribe to, and how exchange symbols map to canonical ones.
CREATE TABLE instrument (
    id                  SERIAL PRIMARY KEY,
    exchange            TEXT        NOT NULL,
    canonical_symbol    TEXT        NOT NULL,   -- BTC/USD
    exchange_symbol     TEXT        NOT NULL,   -- what Kraken calls it on the wire
    base_asset          TEXT        NOT NULL,
    quote_asset         TEXT        NOT NULL,
    tick_size           NUMERIC(38, 18) NOT NULL,
    lot_size            NUMERIC(38, 18) NOT NULL,
    price_precision     INT         NOT NULL,
    quantity_precision  INT         NOT NULL,
    book_depth          INT         NOT NULL DEFAULT 10,  -- phase 3; open question 3 in the design
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT instrument_exchange_canonical_uq UNIQUE (exchange, canonical_symbol),
    CONSTRAINT instrument_exchange_symbol_uq    UNIQUE (exchange, exchange_symbol)
);

CREATE INDEX instrument_enabled_idx ON instrument (exchange) WHERE enabled;

-- Which processor build and schema versions are (and were) live.
-- This is what makes "which code produced this number?" answerable (design doc 22).
CREATE TABLE processor_version (
    id                SERIAL PRIMARY KEY,
    processor_version TEXT        NOT NULL,
    schema_version    INT         NOT NULL,
    git_sha           TEXT,
    deployed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at        TIMESTAMPTZ,
    notes             TEXT
);

CREATE UNIQUE INDEX processor_version_live_idx
    ON processor_version (processor_version) WHERE retired_at IS NULL;

-- Replay-job bookkeeping: offset resets are scripted and logged for auditability
-- (design doc 20.4). Populated from phase 2 onward.
CREATE TABLE replay_job (
    id                SERIAL PRIMARY KEY,
    application_id    TEXT        NOT NULL,
    source_topic      TEXT        NOT NULL,
    target_topic      TEXT        NOT NULL,
    from_offset       BIGINT,
    to_offset         BIGINT,
    from_timestamp    TIMESTAMPTZ,
    to_timestamp      TIMESTAMPTZ,
    processor_version TEXT        NOT NULL,
    requested_by      TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'PENDING',
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT replay_job_status_ck
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'))
);
