package com.marketstream.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Candle;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Topics;
import com.marketstream.schemas.AvroSerdes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives the whole topology in-process. No broker, no registry — a {@code mock://} registry
 * URL gives the Avro serdes a schema store that lives in memory.
 */
class ProcessorTopologyTest {

    private static final String REGISTRY = "mock://processor-topology-test";
    private static final Instant PROCESSED_AT = Instant.parse("2026-08-23T12:05:00Z");

    private TopologyTestDriver driver;
    private ProcessorMetrics metrics;
    private TestInputTopic<String, RawEnvelope> rawTrades;
    private TestOutputTopic<String, TradeEvent> normalizedTrades;
    private TestOutputTopic<String, Candle> candles;
    private TestOutputTopic<String, RawEnvelope> deadLetter;

    @BeforeEach
    void startDriver(@TempDir java.nio.file.Path stateDir) {
        ProcessorConfig config = new ProcessorConfig(
                "localhost:9092", REGISTRY,
                "jdbc:none", "none", "none",
                "market-processor-test", stateDir.toString(),
                0, Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofMillis(0),
                1024 * 1024);

        metrics = new ProcessorMetrics();
        driver = new TopologyTestDriver(
                ProcessorTopology.build(config, "0.1.0-TEST", metrics, () -> PROCESSED_AT),
                StreamsSettings.forProcessor(config));

        Serde<String> keys = Serdes.String();
        Serde<RawEnvelope> raw = AvroSerdes.forValue(REGISTRY);
        Serde<TradeEvent> trades = AvroSerdes.forValue(REGISTRY);
        Serde<Candle> candleSerde = AvroSerdes.forValue(REGISTRY);

        rawTrades = driver.createInputTopic(
                Topics.RAW_KRAKEN_TRADE, keys.serializer(), raw.serializer());
        normalizedTrades = driver.createOutputTopic(
                Topics.NORMALIZED_TRADES, keys.deserializer(), trades.deserializer());
        candles = driver.createOutputTopic(
                Topics.DERIVED_CANDLES, keys.deserializer(), candleSerde.deserializer());
        deadLetter = driver.createOutputTopic(
                Topics.DEAD_LETTER, keys.deserializer(), raw.deserializer());
    }

    @AfterEach
    void stopDriver() {
        if (driver != null) {
            driver.close();
        }
        if (metrics != null) {
            metrics.close();
        }
    }

    @Test
    void turnsARawFrameIntoANormalizedTradeAndAProvisionalCandle() {
        send(trade("BTC/USD", "buy", "60000.0", "2.0", 1, "2026-08-23T12:00:10.000000Z"));

        TradeEvent trade = normalizedTrades.readValue();
        assertThat(trade.getTradeId()).isEqualTo("1");
        assertThat(trade.getHeader().getInstrument()).isEqualTo("BTC/USD");

        List<Candle> emitted = candles.readValuesToList();
        assertThat(emitted).hasSize(1);
        Candle provisional = emitted.get(0);
        assertThat(provisional.getIsFinal()).isFalse();
        assertThat(provisional.getWindowStart()).isEqualTo(Instant.parse("2026-08-23T12:00:00Z"));
        assertThat(provisional.getClose()).isEqualByComparingTo("60000.0");
    }

    @Test
    void keysCandlesByExchangeAndInstrumentSoOneBookStaysOnOnePartition() {
        send(trade("BTC/USD", "buy", "60000.0", "1.0", 1, "2026-08-23T12:00:10.000000Z"));
        send(trade("ETH/USD", "buy", "2600.0", "1.0", 2, "2026-08-23T12:00:11.000000Z"));

        assertThat(candles.readKeyValuesToList()).extracting(record -> record.key)
                .containsExactly("KRAKEN|BTC/USD", "KRAKEN|ETH/USD");
    }

    @Test
    void emitsTheFinalCandleOnceStreamTimeMovesPastTheGrace() {
        send(trade("BTC/USD", "buy", "60000.0", "1.0", 1, "2026-08-23T12:00:10.000000Z"));
        send(trade("BTC/USD", "sell", "60100.0", "3.0", 2, "2026-08-23T12:00:20.000000Z"));
        // Window [12:00, 12:01) closes at 12:01:30 with 30s grace. This trade sits past that
        // and is what advances stream time far enough to release the suppressed result.
        send(trade("BTC/USD", "buy", "60050.0", "1.0", 3, "2026-08-23T12:01:40.000000Z"));

        List<Candle> finals = candles.readValuesToList().stream()
                .filter(Candle::getIsFinal)
                .toList();

        assertThat(finals).hasSize(1);
        Candle candle = finals.get(0);
        assertThat(candle.getWindowStart()).isEqualTo(Instant.parse("2026-08-23T12:00:00Z"));
        assertThat(candle.getOpen()).isEqualByComparingTo("60000.0");
        assertThat(candle.getHigh()).isEqualByComparingTo("60100.0");
        assertThat(candle.getLow()).isEqualByComparingTo("60000.0");
        assertThat(candle.getClose()).isEqualByComparingTo("60100.0");
        assertThat(candle.getVolume()).isEqualByComparingTo("4.0");
        assertThat(candle.getTradeCount()).isEqualTo(2);
        // (60000 * 1 + 60100 * 3) / 4
        assertThat(candle.getVwap()).isEqualByComparingTo("60075");
    }

    @Test
    void leavesAFinalisedCandleAloneWhenATradeArrivesAfterItsGrace() {
        // Correctness invariant 1: a finalised candle for a closed window never changes.
        send(trade("BTC/USD", "buy", "60000.0", "1.0", 1, "2026-08-23T12:00:10.000000Z"));
        send(trade("BTC/USD", "buy", "60000.0", "1.0", 2, "2026-08-23T12:02:00.000000Z"));
        // Belongs in the first window, but arrives long after that window closed past grace.
        send(trade("BTC/USD", "buy", "99999.0", "5.0", 3, "2026-08-23T12:00:30.000000Z"));

        List<Candle> firstWindowFinals = candles.readValuesToList().stream()
                .filter(Candle::getIsFinal)
                .filter(candle -> candle.getWindowStart()
                        .equals(Instant.parse("2026-08-23T12:00:00Z")))
                .toList();

        assertThat(firstWindowFinals).hasSize(1);
        assertThat(firstWindowFinals.get(0).getTradeCount()).isEqualTo(1);
        assertThat(firstWindowFinals.get(0).getHigh()).isEqualByComparingTo("60000.0");
    }

    @Test
    void sendsAMalformedFrameToDeadLetterAndKeepsProcessing() {
        send(envelope("{\"channel\":\"trade\",\"data\":["));
        send(trade("BTC/USD", "buy", "60000.0", "1.0", 1, "2026-08-23T12:00:10.000000Z"));

        assertThat(deadLetter.readValuesToList()).hasSize(1);
        // The topology survived the bad frame: the next one still produced a trade.
        assertThat(normalizedTrades.readValuesToList()).hasSize(1);
    }

    private void send(RawEnvelope envelope) {
        rawTrades.pipeInput(envelope.getInstrument() == null
                ? null
                : "KRAKEN|" + envelope.getInstrument(), envelope);
    }

    private static RawEnvelope trade(
            String symbol, String side, String price, String qty, int tradeId, String timestamp) {

        // receivedAt tracks the trade's own time, as it does on the wire: the ingestor sees
        // a frame milliseconds after the trade happens. Pinning every envelope to one
        // receive time instead would make later trades look like they came from the future,
        // and EventTimeResolver's 60-second skew guard would correctly discard their
        // timestamps — which is a property of the fixture, not of the topology.
        Instant receivedAt = Instant.parse(timestamp).plusMillis(50);
        return envelope(symbol, receivedAt, """
                {"channel":"trade","type":"update","data":[
                  {"symbol":"%s","side":"%s","price":%s,"qty":%s,
                   "ord_type":"market","trade_id":%d,"timestamp":"%s"}]}
                """.formatted(symbol, side, price, qty, tradeId, timestamp));
    }

    private static RawEnvelope envelope(String payload) {
        return envelope("BTC/USD", Instant.parse("2026-08-23T12:00:00Z"), payload);
    }

    private static RawEnvelope envelope(String symbol, Instant receivedAt, String payload) {
        return RawEnvelope.newBuilder()
                .setEventId(UUID.nameUUIDFromBytes(payload.getBytes(StandardCharsets.UTF_8)))
                .setExchange(Exchange.KRAKEN)
                .setChannel("trade")
                .setInstrument(symbol)
                .setReceivedAt(receivedAt)
                .setSourceConnectionId("14134529360570871696")
                .setIngestSequence(1L)
                .setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
                .setPayloadEncoding("json")
                .setTraceId("trace-1")
                .setSchemaVersion(1)
                .build();
    }
}
