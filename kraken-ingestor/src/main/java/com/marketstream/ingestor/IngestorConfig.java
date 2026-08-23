package com.marketstream.ingestor;

import java.time.Duration;

/**
 * Every knob the ingestor has, resolved from the environment once at startup.
 *
 * <p>Defaults target a developer running against {@code make up} with the stack's ports
 * forwarded to localhost. Compose overrides them with in-network hostnames, so the same
 * image runs unchanged in both places.
 */
public record IngestorConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword,
        String websocketUrl,
        int bookDepth,
        int queueCapacity,
        int metricsPort,
        Duration connectTimeout,
        Duration silenceTimeout,
        Duration initialBackoff,
        Duration maxBackoff,
        Duration producerMaxBlock) {

    public static IngestorConfig fromEnv() {
        return new IngestorConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                env("POSTGRES_URL", "jdbc:postgresql://localhost:5432/market"),
                env("POSTGRES_USER", "market"),
                env("POSTGRES_PASSWORD", "market"),
                env("KRAKEN_WS_URL", "wss://ws.kraken.com/v2"),
                envInt("KRAKEN_BOOK_DEPTH", 10),
                envInt("INGEST_QUEUE_CAPACITY", 10_000),
                envInt("METRICS_PORT", 9101),
                envMillis("WS_CONNECT_TIMEOUT_MS", 10_000),
                // Kraken emits a heartbeat roughly every second when a connection is
                // otherwise idle, so ten seconds of total silence means the connection is
                // dead even though TCP has not noticed yet.
                envMillis("WS_SILENCE_TIMEOUT_MS", 10_000),
                envMillis("WS_INITIAL_BACKOFF_MS", 500),
                envMillis("WS_MAX_BACKOFF_MS", 30_000),
                // The backpressure budget. While Kafka is unreachable the producer's
                // buffer fills, send() blocks, and the WebSocket stops reading — which is
                // the behaviour we want. Past this the send fails loudly rather than
                // dropping the frame silently (design doc 18.1).
                envMillis("PRODUCER_MAX_BLOCK_MS", 300_000));
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
