package com.marketstream.schemas;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/**
 * Builds Schema-Registry-aware Avro SerDes configured identically everywhere.
 *
 * <p>Centralised so no service can quietly diverge: a consumer without
 * {@code specific.avro.reader=true} silently deserialises into {@code GenericRecord} and
 * fails at the first cast, far from the misconfiguration.
 *
 * <p><strong>On decimal logical types:</strong> no explicit
 * {@code Conversions.DecimalConversion} registration is needed. Because the schemas are
 * generated with {@code enableDecimalLogicalType=true}, each generated class carries its
 * own conversions and the SerDes use them — verified by {@code AvroRoundTripTest}, which
 * asserts {@link java.math.BigDecimal} identity including scale. If that test ever starts
 * returning {@code ByteBuffer}, this is the place to wire the conversion in.
 */
public final class AvroSerdes {

    private AvroSerdes() {
    }

    /** Base config every producer, consumer and Streams app shares. */
    public static Map<String, Object> config(String schemaRegistryUrl) {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    /** A value SerDe for the given generated record type. */
    public static <T extends SpecificRecord> Serde<T> forValue(String schemaRegistryUrl) {
        return configured(new SpecificAvroSerde<>(), schemaRegistryUrl, false);
    }

    /** A key SerDe. Rarely needed: keys are plain strings ({@code KRAKEN|BTC/USD}). */
    public static <T extends SpecificRecord> Serde<T> forKey(String schemaRegistryUrl) {
        return configured(new SpecificAvroSerde<>(), schemaRegistryUrl, true);
    }

    private static <T extends SpecificRecord> Serde<T> configured(
            SpecificAvroSerde<T> serde, String schemaRegistryUrl, boolean isKey) {
        serde.configure(config(schemaRegistryUrl), isKey);
        return serde;
    }
}
