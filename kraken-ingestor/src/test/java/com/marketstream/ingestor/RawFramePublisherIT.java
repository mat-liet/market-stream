package com.marketstream.ingestor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.marketstream.avro.RawEnvelope;
import com.marketstream.common.Exchange;
import com.marketstream.common.InstrumentKey;
import com.marketstream.common.Topics;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Proves the property everything downstream rests on: a frame published to a raw topic comes
 * back byte-identical, under the key its instrument dictates.
 *
 * <p>Replay is defined as re-running the processor over {@code raw.*}. If the ingestor
 * re-encodes a frame on the way in — reformatting JSON, normalising numbers, even
 * round-tripping through a parser — then replay reprocesses something the exchange never
 * sent, and every "we can rebuild this from raw" guarantee in the design quietly stops
 * holding. Comparing bytes rather than parsed content is the point.
 *
 * <p><strong>Why this runs against {@code make up} rather than Testcontainers:</strong>
 * Testcontainers configures its Kafka containers by uploading a start-up script that exports
 * {@code KAFKA_ADVERTISED_LISTENERS}. On Docker Engine 29 that upload does not take effect,
 * so the broker falls back to deriving its advertised listeners from {@code 0.0.0.0} and
 * refuses to start — for both {@code confluentinc/cp-kafka} and {@code apache/kafka}. Rather
 * than lose the guarantee this test exists for, it runs against the Compose stack the repo
 * already provides, and <em>skips</em> when that stack is down so a plain {@code mvn verify}
 * on a machine with no stack still passes. Revisit when Testcontainers supports Engine 29.
 *
 * <p>Running against Compose is not purely a downgrade: this exercises the real Schema
 * Registry, so schemas are genuinely registered under the {@code BACKWARD} gate rather than
 * against an in-memory mock.
 */
class RawFramePublisherIT {

    private static final String BOOTSTRAP =
            envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String REGISTRY =
            envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    private static IngestorConfig config;

    @BeforeAll
    static void requireTheLocalStack() {
        assumeTrue(kafkaReachable(), "Kafka is not reachable at " + BOOTSTRAP + " — run `make up`");
        assumeTrue(registryReachable(), "Schema Registry is not reachable at " + REGISTRY);
        config = testConfig();
    }

    @Test
    void publishesTheFrameVerbatimUnderItsInstrumentKey() {
        // A frame with everything a re-encoder would "fix": exotic spacing, a price with more
        // precision than a double holds, and a trailing-zero quantity that any float
        // round-trip would destroy.
        String frame = "{\"channel\": \"trade\",\"type\":\"update\",  \"data\":[{"
                + "\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":60123.450000000001,"
                + "\"qty\":0.00100000,\"ord_type\":\"market\",\"trade_id\":70154386,"
                + "\"timestamp\":\"2026-08-23T12:00:00.123456Z\"}]}";
        byte[] payload = frame.getBytes(StandardCharsets.UTF_8);

        InstrumentKey key = InstrumentKey.of(Exchange.KRAKEN, "BTC/USD");
        String connectionId = "13834774380200032777";
        long marker = System.nanoTime();

        try (RawFramePublisher publisher = new RawFramePublisher(config, new IngestorMetrics())) {
            publisher.publish(Topics.RAW_KRAKEN_TRADE, key, "trade", payload, connectionId, marker);
            publisher.flush();
        }

        RawEnvelope envelope = consumeMarked(Topics.RAW_KRAKEN_TRADE, marker, "KRAKEN|BTC/USD");

        assertThat(envelope.getChannel()).isEqualTo("trade");
        assertThat(envelope.getInstrument()).isEqualTo("BTC/USD");
        // Carried as a string: this value does not fit in a long.
        assertThat(envelope.getSourceConnectionId()).isEqualTo(connectionId);
        assertThat(bytesOf(envelope)).isEqualTo(payload);
        assertThat(new String(bytesOf(envelope), StandardCharsets.UTF_8)).isEqualTo(frame);
    }

    @Test
    void keysBookFramesOntoTheirOwnTopic() {
        byte[] payload = ("{\"channel\":\"book\",\"type\":\"snapshot\",\"data\":[{\"symbol\":\"ETH/USD\","
                        + "\"checksum\":2439117997}]}")
                .getBytes(StandardCharsets.UTF_8);
        InstrumentKey key = InstrumentKey.of(Exchange.KRAKEN, "ETH/USD");
        long marker = System.nanoTime();

        try (RawFramePublisher publisher = new RawFramePublisher(config, new IngestorMetrics())) {
            publisher.publish(Topics.RAW_KRAKEN_BOOK, key, "book", payload, "conn-1", marker);
            publisher.flush();
        }

        RawEnvelope envelope = consumeMarked(Topics.RAW_KRAKEN_BOOK, marker, "KRAKEN|ETH/USD");

        assertThat(envelope.getChannel()).isEqualTo("book");
        assertThat(bytesOf(envelope)).isEqualTo(payload);
    }

    @Test
    void storesAnUnkeyableFrameOnDeadLetterRatherThanDroppingIt() {
        byte[] payload = "{\"channel\":\"ticker\"}".getBytes(StandardCharsets.UTF_8);
        long marker = System.nanoTime();

        try (RawFramePublisher publisher = new RawFramePublisher(config, new IngestorMetrics())) {
            publisher.publish(Topics.DEAD_LETTER, null, "ticker", payload, "conn-1", marker);
            publisher.flush();
        }

        // A null key is only acceptable here, where nothing depends on ordering.
        RawEnvelope envelope = consumeMarked(Topics.DEAD_LETTER, marker, null);

        assertThat(envelope.getInstrument()).isNull();
        assertThat(bytesOf(envelope)).isEqualTo(payload);
    }

    private static byte[] bytesOf(RawEnvelope envelope) {
        byte[] stored = new byte[envelope.getPayload().remaining()];
        envelope.getPayload().duplicate().get(stored);
        return stored;
    }

    /**
     * Reads the topic from the beginning and returns the record this test wrote.
     *
     * <p>The topic is shared with whatever else has been running locally, so records are
     * matched on the unique {@code ingestSequence} marker rather than by taking the first
     * one — otherwise a live ingestor in the background would make this test flap.
     */
    private static RawEnvelope consumeMarked(String topic, long marker, String expectedKey) {
        Properties properties = new Properties();
        properties.put("schema.registry.url", REGISTRY);
        properties.put("specific.avro.reader", true);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kraken-ingestor-it-" + marker);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        try (KafkaConsumer<String, RawEnvelope> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, RawEnvelope> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RawEnvelope> record : records) {
                    if (record.value() != null && record.value().getIngestSequence() == marker) {
                        assertThat(record.key()).isEqualTo(expectedKey);
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("no record marked " + marker + " arrived on " + topic + " within 60s");
    }

    private static IngestorConfig testConfig() {
        return new IngestorConfig(
                BOOTSTRAP,
                REGISTRY,
                "unused",
                "unused",
                "unused",
                "unused",
                10,
                100,
                0,
                Duration.ofSeconds(5),
                Duration.ofSeconds(10),
                Duration.ofMillis(500),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30));
    }

    private static boolean kafkaReachable() {
        String[] parts = BOOTSTRAP.split(":", 2);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), 2000);
            return true;
        } catch (IOException | NumberFormatException e) {
            return false;
        }
    }

    private static boolean registryReachable() {
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(REGISTRY + "/subjects"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
