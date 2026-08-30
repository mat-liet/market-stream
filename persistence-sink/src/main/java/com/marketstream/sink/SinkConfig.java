package com.marketstream.sink;

import java.time.Duration;

/**
 * Every knob the sink has, resolved from the environment once at startup.
 *
 * <p>Defaults target a developer running against {@code make up} with the stack's ports
 * forwarded to localhost. Compose overrides them with in-network hostnames, so the same image
 * runs unchanged in both places.
 *
 * <p>The two batch sizes differ by an order of magnitude on purpose: {@code normalized.trades}
 * carries every trade on the exchange, while {@code derived.candles} carries a handful of
 * records per minute per instrument. A shared figure would either make the candle sink wait
 * for a batch that never fills or make the trade sink insert in wastefully small blocks —
 * and ClickHouse strongly prefers large inserts (design doc 15.5).
 */
public record SinkConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String clickHouseUrl,
        String clickHouseUser,
        String clickHousePassword,
        String groupIdPrefix,
        int metricsPort,
        int tradeBatchRows,
        int candleBatchRows,
        Duration flushInterval,
        Duration pollTimeout,
        Duration initialRetryBackoff,
        Duration maxRetryBackoff) {

    public static SinkConfig fromEnv() {
        return new SinkConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                env("CLICKHOUSE_URL", "jdbc:ch://localhost:8123/market"),
                env("CLICKHOUSE_USER", "market"),
                env("CLICKHOUSE_PASSWORD", "market"),
                // Each sink appends its table name, so the two loops have independent
                // offsets. Changing this replays both topics from the start of retention —
                // harmless, because the writes are idempotent, but slow.
                env("SINK_GROUP_ID_PREFIX", "persistence-sink"),
                envInt("METRICS_PORT", 9103),
                envInt("TRADE_BATCH_ROWS", 5_000),
                envInt("CANDLE_BATCH_ROWS", 500),
                // The upper bound on how long a row can sit unwritten. Without it a quiet
                // instrument's last candle would wait for a batch that never fills.
                envMillis("SINK_FLUSH_INTERVAL_MS", 2_000),
                envMillis("SINK_POLL_TIMEOUT_MS", 200),
                envMillis("SINK_RETRY_INITIAL_MS", 500),
                // Long enough that a ClickHouse restart does not produce a wall of failed
                // attempts, short enough that recovery is noticed within a scrape interval.
                envMillis("SINK_RETRY_MAX_MS", 30_000));
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got: '" + value + "'", e);
        }
    }

    private static Duration envMillis(String name, long fallbackMillis) {
        return Duration.ofMillis(envInt(name, Math.toIntExact(fallbackMillis)));
    }
}
