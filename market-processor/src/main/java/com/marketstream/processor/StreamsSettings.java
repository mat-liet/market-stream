package com.marketstream.processor;

import com.marketstream.schemas.AvroSerdes;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

/**
 * The Streams configuration (design doc 13.3).
 *
 * <p>Starts from {@link AvroSerdes#config} for the same reason the ingestor's producer does:
 * the schema registry URL and {@code specific.avro.reader} have to be right everywhere, and
 * a consumer missing the latter silently deserialises into {@code GenericRecord} and fails
 * at the first cast.
 */
public final class StreamsSettings {

    private StreamsSettings() {
    }

    public static Properties forProcessor(ProcessorConfig config) {
        Map<String, Object> settings = new HashMap<>(AvroSerdes.config(config.schemaRegistryUrl()));

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // The whole point of running Streams here. Consuming a raw record, updating the
        // window store and producing the normalised trade and the candle all commit as one
        // transaction, so a crash mid-flight rolls back rather than leaving a partial
        // candle behind (design doc 19.2).
        settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.commitInterval().toMillis());

        // Phase 1 runs a single instance, so a standby would only cost disk. Phase 4 raises
        // this once the book store makes cold restores expensive.
        settings.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);

        settings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        settings.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        // At most five in flight so a retry cannot reorder records within a partition, which
        // would break per-book ordering (design doc 17.1).
        settings.put(
                StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 5);
        settings.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");

        // Only committed records are readable, so the second sub-topology never sees a trade
        // from a transaction that later aborted. EOS sets this itself; stating it makes the
        // guarantee legible rather than implied.
        settings.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
                "read_committed");

        // A record that will not deserialise must not take the topology down (design doc 18,
        // scenario 8). This is a remote case — raw records are written by our own producer
        // against the same registry — and it logs rather than dropping quietly. Phase 2
        // replaces it with a handler that routes to dead-letter instead of only logging.
        settings.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        Properties properties = new Properties();
        properties.putAll(settings);
        return properties;
    }
}
