package com.marketstream.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.marketstream.avro.Candle;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.InvalidEvent;
import com.marketstream.avro.InvalidReason;
import com.marketstream.avro.ProcessingStage;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.common.Topics;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Drives the deployed processor: real broker, real Schema Registry, real EOS transactions.
 *
 * <p>{@link ProcessorTopologyTest} already proves the topology's logic in-process, so what
 * is left to prove is everything the test driver stands in for — that the schemas register
 * under the {@code BACKWARD} gate, that candles are produced inside transactions a
 * {@code read_committed} consumer can see, and that the container running right now actually
 * does this. It therefore exercises the running {@code market-processor} rather than
 * starting a second Streams app, which would consume the same raw topic and duplicate every
 * candle onto {@code derived.candles} for as long as the test ran.
 *
 * <p>Like {@code kraken-ingestor}'s IT, it runs against the Compose stack rather than
 * Testcontainers, which cannot start a Kafka container on Docker Engine 29, and it
 * <em>skips</em> rather than fails when the stack is not up so a plain {@code mvn verify}
 * still passes on a machine with no Docker.
 *
 * <p>Every run uses a fresh synthetic instrument. That keeps its records on one partition,
 * away from the live BTC/USD and ETH/USD flow, so the test controls that partition's stream
 * time by itself — which is the only way to make a stream-time-driven finalisation happen on
 * demand.
 */
class ProcessorEosIT {

    private static final String BOOTSTRAP =
            envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String REGISTRY =
            envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    private static final String PROCESSOR_HEALTH =
            envOrDefault("PROCESSOR_HEALTH_URL", "http://localhost:9102/health");

    @BeforeAll
    static void requireTheLocalStack() {
        assumeTrue(kafkaReachable(), "Kafka is not reachable at " + BOOTSTRAP + " — run `make up`");
        assumeTrue(registryReachable(), "Schema Registry is not reachable at " + REGISTRY);
        assumeTrue(processorRunning(),
                "market-processor is not RUNNING at " + PROCESSOR_HEALTH + " — run `make up-services`");
    }

    @Test
    void turnsRawFramesIntoAFinalisedCandleWithTheRightArithmetic() {
        // One minute in the recent past, so no event time is in the future and none is
        // beyond the 24-hour staleness bound.
        Instant windowStart = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MINUTES);
        String instrument = "TST" + (System.nanoTime() % 100_000) + "/USD";
        String key = "KRAKEN|" + instrument;

        List<ProducerRecord<String, RawEnvelope>> frames = List.of(
                frame(key, instrument, "buy", "100.5", "2", windowStart.plusSeconds(10), 1),
                frame(key, instrument, "sell", "101.5", "1", windowStart.plusSeconds(20), 2),
                frame(key, instrument, "buy", "99.5", "1", windowStart.plusSeconds(30), 3),
                // Two minutes on: pushes this partition's stream time past windowEnd + 30s
                // grace, which is what releases the suppressed final result.
                frame(key, instrument, "buy", "200", "1", windowStart.plusSeconds(120), 4));

        try (Producer<String, RawEnvelope> producer = new KafkaProducer<>(producerConfig())) {
            frames.forEach(producer::send);
            producer.flush();
        }

        Candle candle = awaitFinalCandle(key, windowStart);

        assertThat(candle.getWindowStart()).isEqualTo(windowStart);
        assertThat(candle.getWindowEnd()).isEqualTo(windowStart.plusSeconds(60));
        assertThat(candle.getOpen()).isEqualByComparingTo("100.5");
        assertThat(candle.getHigh()).isEqualByComparingTo("101.5");
        assertThat(candle.getLow()).isEqualByComparingTo("99.5");
        assertThat(candle.getClose()).isEqualByComparingTo("99.5");
        assertThat(candle.getVolume()).isEqualByComparingTo("4");
        assertThat(candle.getBuyVolume()).isEqualByComparingTo("3");
        assertThat(candle.getSellVolume()).isEqualByComparingTo("1");
        assertThat(candle.getTradeCount()).isEqualTo(3);
        // (100.5*2 + 101.5*1 + 99.5*1) / 4 = 402 / 4
        assertThat(candle.getQuoteVolume()).isEqualByComparingTo("402");
        assertThat(candle.getVwap()).isEqualByComparingTo("100.5");

        // Stamped from the registry row rather than made up by the process, so a wrong
        // number is traceable to a registered deploy.
        assertThat(candle.getProcessorVersion()).isNotBlank();
        assertThat(candle.getHeader().getEventTime()).isEqualTo(windowStart);
    }

    @Test
    void forwardsAnUnparseableFrameToDeadLetterAndKeepsRunning() {
        // dead-letter's registry subject is bound to RawEnvelope by the ingestor, so this
        // also proves the processor produces a schema the BACKWARD gate accepts there —
        // something a mock registry cannot tell us.
        String instrument = "TST" + (System.nanoTime() % 100_000) + "/USD";
        String key = "KRAKEN|" + instrument;
        long sequence = System.nanoTime();

        RawEnvelope broken = envelope(instrument, "{\"channel\":\"trade\",\"data\":[", sequence);
        try (Producer<String, RawEnvelope> producer = new KafkaProducer<>(producerConfig())) {
            producer.send(new ProducerRecord<>(Topics.RAW_KRAKEN_TRADE, key, broken));
            producer.flush();
        }

        RawEnvelope forwarded = awaitMarked(Topics.DEAD_LETTER, sequence);

        // Verbatim: the bytes, and the connection coordinates that make the bad frame
        // findable again in the raw topic.
        assertThat(bytesOf(forwarded)).isEqualTo(bytesOf(broken));
        assertThat(forwarded.getSourceConnectionId()).isEqualTo("processor-eos-it");

        assertThat(processorRunning()).as("the topology survived the bad frame").isTrue();
    }

    @Test
    void sendsAFrameItCanCategoriseToInvalidEventsInstead() {
        // The distinction the two ops topics exist for: this one parsed fine, we just have
        // no idea what a ticker frame means on a trade topic (design doc 11.7).
        String instrument = "TST" + (System.nanoTime() % 100_000) + "/USD";
        String key = "KRAKEN|" + instrument;
        long sequence = System.nanoTime();

        String payload = "{\"channel\":\"ticker\",\"type\":\"update\",\"data\":[{\"symbol\":\""
                + instrument + "\",\"last\":100.0}]}";
        try (Producer<String, RawEnvelope> producer = new KafkaProducer<>(producerConfig())) {
            producer.send(new ProducerRecord<>(
                    Topics.RAW_KRAKEN_TRADE, key, envelope(instrument, payload, sequence)));
            producer.flush();
        }

        InvalidEvent invalid = awaitInvalid(sequence);

        assertThat(invalid.getReason()).isEqualTo(InvalidReason.UNKNOWN_TYPE);
        assertThat(invalid.getStage()).isEqualTo(ProcessingStage.NORMALIZE);
        assertThat(invalid.getOriginalTopic()).isEqualTo(Topics.RAW_KRAKEN_TRADE);
        assertThat(invalid.getProcessorVersion()).isNotBlank();
    }

    private static byte[] bytesOf(RawEnvelope envelope) {
        ByteBuffer buffer = envelope.getPayload().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    /** Waits for the envelope this test wrote, matched on its unique ingestSequence. */
    private static RawEnvelope awaitMarked(String topic, long marker) {
        return await(topic, RawEnvelope.class,
                envelope -> envelope.getIngestSequence() == marker,
                "envelope marked " + marker);
    }

    private static InvalidEvent awaitInvalid(long marker) {
        // The rejection carries no ingestSequence of its own, so it is matched on the trace
        // id the envelope carried in.
        return await(Topics.INVALID_EVENTS, InvalidEvent.class,
                invalid -> ("it-" + marker).equals(invalid.getTraceId()),
                "rejection for trace it-" + marker);
    }

    private static <T> T await(
            String topic, Class<T> type, java.util.function.Predicate<T> matches, String what) {

        try (KafkaConsumer<String, T> consumer = new KafkaConsumer<>(consumerConfig())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, T> record : records) {
                    T value = record.value();
                    if (value != null && type.isInstance(value) && matches.test(value)) {
                        return value;
                    }
                }
            }
        }
        throw new AssertionError("no " + what + " arrived on " + topic + " within 60s");
    }

    private static Properties consumerConfig() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", REGISTRY);
        properties.put("specific.avro.reader", true);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "market-processor-it-" + System.nanoTime());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        return properties;
    }

    /**
     * Reads with {@code read_committed}, so anything visible here is committed output of a
     * completed EOS transaction rather than a record that a crash could still roll back.
     */
    private static Candle awaitFinalCandle(String key, Instant windowStart) {
        List<Candle> matches = new ArrayList<>();
        try (KafkaConsumer<String, Candle> consumer = new KafkaConsumer<>(consumerConfig())) {
            consumer.subscribe(List.of(Topics.DERIVED_CANDLES));
            long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, Candle> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Candle> record : records) {
                    Candle candle = record.value();
                    if (candle != null
                            && key.equals(record.key())
                            && candle.getIsFinal()
                            && candle.getWindowStart().equals(windowStart)) {
                        matches.add(candle);
                    }
                }
                if (!matches.isEmpty()) {
                    // Correctness invariant 1: a window finalises once. Keep reading briefly
                    // so a second one would be caught rather than missed by returning early.
                    consumer.poll(Duration.ofSeconds(2));
                    break;
                }
            }
        }

        assertThat(matches)
                .as("no finalised candle for %s at %s arrived within 90s", key, windowStart)
                .hasSize(1);
        return matches.get(0);
    }

    private static ProducerRecord<String, RawEnvelope> frame(
            String key, String instrument, String side, String price, String qty,
            Instant eventTime, long sequence) {

        String payload = """
                {"channel":"trade","type":"update","data":[\
                {"symbol":"%s","side":"%s","price":%s,"qty":%s,\
                "ord_type":"market","trade_id":%d,"timestamp":"%s"}]}"""
                .formatted(instrument, side, price, qty, sequence, eventTime.toString());

        // receivedAt tracks the trade's own time, as it does on the wire. A single fixed
        // receive time would make the later frames look like they came from the future, and
        // the skew guard would correctly discard their timestamps.
        return new ProducerRecord<>(Topics.RAW_KRAKEN_TRADE, key,
                envelope(instrument, payload, sequence, eventTime.plusMillis(50)));
    }

    private static RawEnvelope envelope(String instrument, String payload, long sequence) {
        return envelope(instrument, payload, sequence, Instant.now());
    }

    private static RawEnvelope envelope(
            String instrument, String payload, long sequence, Instant receivedAt) {

        return RawEnvelope.newBuilder()
                .setEventId(UUID.randomUUID())
                .setExchange(Exchange.KRAKEN)
                .setChannel("trade")
                .setInstrument(instrument)
                .setReceivedAt(receivedAt)
                .setSourceConnectionId("processor-eos-it")
                .setIngestSequence(sequence)
                .setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
                .setPayloadEncoding("json")
                .setTraceId("it-" + sequence)
                .setSchemaVersion(1)
                .build();
    }

    private static Properties producerConfig() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", REGISTRY);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
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
        return get(REGISTRY + "/subjects") != null;
    }

    /** /health returns the Streams state, so this skips on a processor that is not processing. */
    private static boolean processorRunning() {
        String state = get(PROCESSOR_HEALTH);
        return "RUNNING".equals(state);
    }

    private static String get(String url) {
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build()) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : null;
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
