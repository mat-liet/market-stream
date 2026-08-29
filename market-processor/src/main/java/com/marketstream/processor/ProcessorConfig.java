package com.marketstream.processor;

import java.time.Duration;

/**
 * Every knob the processor has, resolved from the environment once at startup.
 *
 * <p>Defaults target a developer running against {@code make up} with the stack's ports
 * forwarded to localhost. Compose overrides them with in-network hostnames, so the same
 * image runs unchanged in both places.
 */
public record ProcessorConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword,
        String applicationId,
        String stateDir,
        int metricsPort,
        Duration candleWindow,
        Duration candleGrace,
        Duration commitInterval,
        long suppressBufferBytes) {

    public static ProcessorConfig fromEnv() {
        return new ProcessorConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                env("POSTGRES_URL", "jdbc:postgresql://localhost:5432/market"),
                env("POSTGRES_USER", "market"),
                env("POSTGRES_PASSWORD", "market"),
                // Also the consumer group id and the prefix of every changelog topic.
                // Changing it orphans all existing state (design doc 20.4).
                env("STREAMS_APPLICATION_ID", "market-processor"),
                env("STREAMS_STATE_DIR", "/var/lib/market-processor"),
                envInt("METRICS_PORT", 9102),
                envMillis("CANDLE_WINDOW_MS", 60_000),
                // Design doc 13.1 proposes 15-30s for the 1m window. The 1m candle is the
                // storage-of-record candle, so completeness beats latency and we take the
                // top of that range. Open question 4 wants this retuned against a real
                // late-arrival distribution once we have captured one.
                envMillis("CANDLE_GRACE_MS", 30_000),
                // Under EOS the Streams default is 100ms. The commit interval sets both the
                // provisional-candle emission rate and how long a normalised trade stays
                // invisible to the second sub-topology, since read_committed cannot see an
                // open transaction. 500ms cuts transaction overhead fivefold at a latency
                // cost nothing here notices.
                envMillis("STREAMS_COMMIT_INTERVAL_MS", 500),
                // The suppression buffer holds one open window per instrument, so 32MB is
                // enormous for two instruments. It is bounded anyway: an unbounded buffer
                // fails as an OOM kill, which looks like a crash rather than a capacity
                // problem.
                envInt("SUPPRESS_BUFFER_BYTES", 32 * 1024 * 1024));
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
