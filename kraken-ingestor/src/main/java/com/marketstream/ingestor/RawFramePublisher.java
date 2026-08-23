package com.marketstream.ingestor;

import com.marketstream.avro.RawEnvelope;
import com.marketstream.common.InstrumentKey;
import com.marketstream.schemas.AvroSerdes;
import com.marketstream.schemas.SchemaVersions;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps each frame in a {@link RawEnvelope} and produces it to its raw topic.
 *
 * <p>The one invariant worth defending here: {@code payload} is the bytes that came off the
 * socket, unmodified. Not re-serialised, not pretty-printed, not normalised. Everything
 * downstream is defined as a deterministic function of these topics, so re-encoding a frame
 * — even into equivalent JSON — quietly makes a replay produce something the original run
 * never saw.
 *
 * <p>Producer settings are chosen so that loss is impossible and blocking is acceptable:
 * {@code acks=all} with idempotence for the former, a large {@code max.block.ms} for the
 * latter. When Kafka is unreachable, {@code send} blocks on a full buffer, the caller stops
 * draining, and the WebSocket stops reading — backpressure all the way to the exchange
 * rather than a silent drop (design doc 18.1, scenario 3).
 */
public final class RawFramePublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RawFramePublisher.class);

    private final Producer<String, RawEnvelope> producer;
    private final IngestorMetrics metrics;

    public RawFramePublisher(IngestorConfig config, IngestorMetrics metrics) {
        this(new KafkaProducer<>(producerConfig(config)), metrics);
    }

    /** Visible for the integration test, which supplies a producer aimed at a container. */
    RawFramePublisher(Producer<String, RawEnvelope> producer, IngestorMetrics metrics) {
        this.producer = producer;
        this.metrics = metrics;
    }

    static Map<String, Object> producerConfig(IngestorConfig config) {
        Map<String, Object> properties = new HashMap<>(AvroSerdes.config(config.schemaRegistryUrl()));
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kraken-ingestor");
        // Durability over latency: raw is the only copy of what the exchange said.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        // A little batching costs a few milliseconds and roughly halves the request rate
        // during busy markets, which is when the ingestor is under most pressure.
        int lingerMs = 5;
        int requestTimeoutMs = 30_000;
        properties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.producerMaxBlock().toMillis());
        // Set explicitly because the default — 2 minutes — is a silent data-loss window. A
        // record already accepted into the producer buffer is expired once this elapses, and
        // during a four-minute broker outage that discarded 558 already-received frames,
        // which showed up as exactly 558 gaps in that connection's ingestSequence.
        // Backpressure (full buffer, then full queue, then an unread socket) is what is meant
        // to absorb an outage, so the delivery budget has to outlast the blocking budget
        // rather than undercut it. Kafka rejects a value below linger + request timeout, so
        // the floor is applied here instead of letting a small configured budget refuse to
        // start the producer at all.
        properties.put(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                Math.toIntExact(Math.max(
                        config.producerMaxBlock().toMillis(), (long) lingerMs + requestTimeoutMs)));
        return properties;
    }

    /**
     * Publishes one frame. Blocks when the producer buffer is full — that is the intent.
     *
     * @param key     the partition key, or null for a frame whose instrument could not be
     *                resolved. Null keys are only ever used on {@code dead-letter}, which
     *                has no ordering requirement; putting an unkeyed record on a data topic
     *                would round-robin it across partitions and break per-book ordering for
     *                everything else on that book.
     * @param payload the frame exactly as received; stored without modification
     */
    public void publish(
            String topic,
            InstrumentKey key,
            String channel,
            byte[] payload,
            String connectionId,
            long ingestSequence) {

        RawEnvelope envelope = RawEnvelope.newBuilder()
                .setEventId(UUID.randomUUID())
                .setExchange(com.marketstream.avro.Exchange.KRAKEN)
                .setChannel(channel)
                .setInstrument(key == null ? null : key.instrument())
                .setReceivedAt(Instant.now())
                .setSourceConnectionId(connectionId)
                .setIngestSequence(ingestSequence)
                .setPayload(ByteBuffer.wrap(payload))
                .setPayloadEncoding("json")
                .setTraceId(UUID.randomUUID().toString())
                .setSchemaVersion(SchemaVersions.CURRENT)
                .build();

        String recordKey = key == null ? null : key.asKafkaKey();
        producer.send(new ProducerRecord<>(topic, recordKey, envelope), (metadata, error) -> {
            if (error != null) {
                // Nothing to retry by hand: the producer has already exhausted its own
                // retries. Surfacing it as a counter is what makes the loss visible.
                metrics.publishFailed(topic);
                log.error("failed to publish to {} key={}", topic, recordKey, error);
            } else {
                metrics.framePublished(topic);
            }
        });
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        // Give in-flight records a chance to land before the process goes away.
        producer.close();
    }
}
