-- ClickHouse schema for the phase 1 walking skeleton (design doc 15.1, 15.5).
--
-- Both tables are ReplacingMergeTree keyed by natural identity, so a replay or
-- an at-least-once redelivery from the sink overwrites rather than duplicates
-- (design doc 19.2: at-least-once + idempotent write = effectively-once).
--
-- `written_at` is the ReplacingMergeTree version column: on merge, the row with
-- the highest version wins. Queries that must not observe pre-merge duplicates
-- use FINAL or aggregate them away.

CREATE DATABASE IF NOT EXISTS market;

-- Historical trades, copied from normalized.trades.
CREATE TABLE IF NOT EXISTS market.trades
(
    exchange        LowCardinality(String),
    instrument      LowCardinality(String),
    trade_id        String,
    event_id        UUID,

    event_time      DateTime64(3),          -- exchange event time: the authoritative business time
    ingestion_time  DateTime64(3),          -- when the ingestor received the frame
    event_time_source LowCardinality(String), -- EXCHANGE | INGESTION_FALLBACK

    price           Decimal(38, 18),        -- never Float: rounding drift is unacceptable here
    quantity        Decimal(38, 18),
    side            LowCardinality(String), -- BUY | SELL (aggressor)

    trace_id        String,
    written_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(written_at)
PARTITION BY toDate(event_time)
ORDER BY (exchange, instrument, event_time, trade_id)
TTL toDateTime(event_time) + INTERVAL 6 MONTH;

-- Candles, copied from derived.candles. The sink writes only isFinal=true rows
-- in phase 1, so every row here is immutable by construction (invariant 1).
CREATE TABLE IF NOT EXISTS market.candles
(
    exchange          LowCardinality(String),
    instrument        LowCardinality(String),
    window            LowCardinality(String),   -- S1 | S10 | M1
    window_start      DateTime64(3),
    window_end        DateTime64(3),

    open              Decimal(38, 18),
    high              Decimal(38, 18),
    low               Decimal(38, 18),
    close             Decimal(38, 18),

    volume            Decimal(38, 18),          -- base-asset volume
    quote_volume      Decimal(38, 18),          -- VWAP numerator
    vwap              Decimal(38, 18),
    buy_volume        Decimal(38, 18),
    sell_volume       Decimal(38, 18),

    trade_count       UInt32,
    input_trade_count UInt32,                   -- trades actually folded, for late-arrival auditing

    processor_version String,                   -- which build produced this row (design doc 22, auditability)
    written_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(written_at)
PARTITION BY toDate(window_start)
ORDER BY (exchange, instrument, window, window_start);
-- No TTL: 1m candles are the storage of record and are kept indefinitely.
