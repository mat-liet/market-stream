package com.marketstream.schemas;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Candle;
import com.marketstream.avro.CandleWindow;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.EventTimeSource;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.InvalidEvent;
import com.marketstream.avro.InvalidReason;
import com.marketstream.avro.ProcessingStage;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import com.marketstream.common.Topics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Proves every schema survives a Schema-Registry round trip with its decimal fields
 * intact as {@link BigDecimal}.
 *
 * <p>This test exists because Confluent's Avro SerDes will hand back {@code ByteBuffer}
 * instead of {@code BigDecimal} if the decimal logical-type conversion is not wired up.
 * That failure is silent at this layer and only surfaces deep inside the Streams topology,
 * so it is pinned down here where the feedback loop is seconds long.
 */
class AvroRoundTripTest {

    private static final Instant EVENT_TIME = Instant.parse("2026-08-22T12:00:00Z");
    private static final Instant INGESTED = EVENT_TIME.plusMillis(30);

    /** mock:// gives each test scope its own in-memory registry, no container needed. */
    private static <T extends org.apache.avro.specific.SpecificRecord> Serde<T> serde() {
        Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(
                        "schema.registry.url", "mock://m1-round-trip",
                        "specific.avro.reader", "true"),
                false);
        return serde;
    }

    private static EventHeader header() {
        return EventHeader.newBuilder()
                .setEventId(UUID.randomUUID())
                .setExchange(Exchange.KRAKEN)
                .setInstrument("BTC/USD")
                .setEventTime(EVENT_TIME)
                .setIngestionTime(INGESTED)
                .setProcessingTime(null)
                .setEventTimeSource(EventTimeSource.EXCHANGE)
                .setSchemaVersion(1)
                .setTraceId("trace-1")
                .setSourceEventId(null)
                .build();
    }

    @Test
    @DisplayName("TradeEvent round-trips with price as BigDecimal at canonical scale")
    void tradeEventRoundTrips() {
        BigDecimal price = Decimals.parse("64250.10");
        BigDecimal quantity = Decimals.parse("0.00123456");

        TradeEvent original = TradeEvent.newBuilder()
                .setHeader(header())
                .setTradeId("12345")
                .setPrice(price)
                .setQuantity(quantity)
                .setSide(Side.BUY)
                .setDedupeKey("BTC/USD|12345")
                .build();

        try (Serde<TradeEvent> serde = serde()) {
            byte[] bytes = serde.serializer().serialize(Topics.NORMALIZED_TRADES, original);
            TradeEvent result = serde.deserializer().deserialize(Topics.NORMALIZED_TRADES, bytes);

            assertThat(result).isEqualTo(original);

            // The point of the test: a decimal, not a ByteBuffer.
            assertThat((Object) result.getPrice()).isInstanceOf(BigDecimal.class);
            assertThat((Object) result.getPrice()).isNotInstanceOf(ByteBuffer.class);

            // equals(), not compareTo(): scale must survive, so 100.5 != 100.50.
            assertThat(result.getPrice()).isEqualTo(price);
            assertThat(result.getPrice().scale()).isEqualTo(Decimals.SCALE);
            assertThat(result.getQuantity()).isEqualTo(quantity);
        }
    }

    @Test
    @DisplayName("Candle round-trips all thirteen decimal fields")
    void candleRoundTrips() {
        Candle original = Candle.newBuilder()
                .setHeader(header())
                .setWindow(CandleWindow.M1)
                .setWindowStart(EVENT_TIME)
                .setWindowEnd(EVENT_TIME.plusSeconds(60))
                .setOpen(Decimals.parse("64000"))
                .setHigh(Decimals.parse("64500.5"))
                .setLow(Decimals.parse("63900.25"))
                .setClose(Decimals.parse("64250.10"))
                .setVolume(Decimals.parse("12.5"))
                .setQuoteVolume(Decimals.parse("803125"))
                .setVwap(Decimals.divide(Decimals.parse("803125"), Decimals.parse("12.5")))
                .setBuyVolume(Decimals.parse("7.25"))
                .setSellVolume(Decimals.parse("5.25"))
                .setTradeCount(42)
                .setIsFinal(true)
                .setInputTradeCount(42)
                .setProcessorVersion("0.1.0-SNAPSHOT")
                .build();

        try (Serde<Candle> serde = serde()) {
            byte[] bytes = serde.serializer().serialize(Topics.DERIVED_CANDLES, original);
            Candle result = serde.deserializer().deserialize(Topics.DERIVED_CANDLES, bytes);

            assertThat(result).isEqualTo(original);
            assertThat(result.getVwap()).isEqualTo(Decimals.parse("64250"));
            assertThat(result.getVwap().scale()).isEqualTo(Decimals.SCALE);
        }
    }

    @Test
    @DisplayName("RawEnvelope round-trips its payload byte-for-byte")
    void rawEnvelopeRoundTrips() {
        byte[] frame = "{\"channel\":\"trade\",\"data\":[]}".getBytes(StandardCharsets.UTF_8);

        RawEnvelope original = RawEnvelope.newBuilder()
                .setEventId(UUID.randomUUID())
                .setExchange(Exchange.KRAKEN)
                .setChannel("trade")
                .setInstrument("BTC/USD")
                .setReceivedAt(INGESTED)
                .setSourceConnectionId("conn-1")
                .setIngestSequence(1_000L)
                .setPayload(ByteBuffer.wrap(frame))
                .setPayloadEncoding("json")
                .setTraceId("trace-1")
                .setSchemaVersion(1)
                .build();

        try (Serde<RawEnvelope> serde = serde()) {
            byte[] bytes = serde.serializer().serialize(Topics.RAW_KRAKEN_TRADE, original);
            RawEnvelope result = serde.deserializer().deserialize(Topics.RAW_KRAKEN_TRADE, bytes);

            assertThat(result).isEqualTo(original);
            // Replay depends on the frame surviving verbatim.
            assertThat(result.getPayload().array()).isEqualTo(frame);
        }
    }

    @Test
    @DisplayName("InvalidEvent round-trips with its optional fields null")
    void invalidEventRoundTrips() {
        InvalidEvent original = InvalidEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setOccurredAt(INGESTED)
                .setStage(ProcessingStage.NORMALIZE)
                .setReason(InvalidReason.PARSE_ERROR)
                .setErrorClass("com.fasterxml.jackson.core.JsonParseException")
                .setOriginalTopic(Topics.RAW_KRAKEN_TRADE)
                .setOriginalKey("KRAKEN|BTC/USD")
                .setOriginalPayload(ByteBuffer.wrap("not json".getBytes(StandardCharsets.UTF_8)))
                .setInstrument(null)
                .setTraceId(null)
                .setProcessorVersion("0.1.0-SNAPSHOT")
                .setSchemaVersion(1)
                .build();

        try (Serde<InvalidEvent> serde = serde()) {
            byte[] bytes = serde.serializer().serialize(Topics.INVALID_EVENTS, original);
            InvalidEvent result = serde.deserializer().deserialize(Topics.INVALID_EVENTS, bytes);

            assertThat(result).isEqualTo(original);
            assertThat(result.getInstrument()).isNull();
        }
    }
}
