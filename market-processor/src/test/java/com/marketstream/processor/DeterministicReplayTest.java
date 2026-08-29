package com.marketstream.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketstream.avro.Candle;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.common.Decimals;
import com.marketstream.common.Topics;
import com.marketstream.schemas.AvroSerdes;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Replays real captured Kraken frames and asserts the candles are both correct and
 * reproducible. This is the phase 1 completion criterion (design doc 26, 29) and the
 * regression backbone every later phase leans on: any change to normalisation or
 * aggregation has to keep this green.
 *
 * <p>Two independent claims are checked. Correctness is checked against an oracle computed
 * here from the same frames by straightforward means — no shared code with
 * {@link CandleAggregator}, so a bug would have to be made twice to hide. Reproducibility
 * (correctness invariant 6) is checked by running the fixture through two fresh drivers and
 * comparing whole records.
 */
class DeterministicReplayTest {

    private static final String FIXTURE = "/fixtures/kraken-btcusd-trades.tsv";
    private static final String REGISTRY = "mock://deterministic-replay-test";
    private static final String KEY = "KRAKEN|BTC/USD";
    private static final Instant PROCESSED_AT = Instant.parse("2026-08-23T14:00:00Z");

    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Test
    void producesTheSameFinalisedCandlesOnEveryReplay(@TempDir Path first, @TempDir Path second) {
        List<RawEnvelope> frames = fixture();

        Map<Instant, Candle> once = finalCandles(frames, first);
        Map<Instant, Candle> twice = finalCandles(frames, second);

        // Whole-record equality, every field included. The one field that legitimately
        // varies between runs is processingTime, and it does not vary here because the
        // topology takes its clock as an argument — which is the reason it does.
        assertThat(once).isEqualTo(twice);
        assertThat(once).isNotEmpty();
    }

    @Test
    void computesOhlcvAndVwapThatMatchTheTradesInTheFrames(@TempDir Path stateDir) {
        List<RawEnvelope> frames = fixture();
        Map<Instant, Candle> produced = finalCandles(frames, stateDir);
        Map<Instant, Oracle> expected = oracle(frames);

        assertThat(produced).isNotEmpty();
        produced.forEach((windowStart, candle) -> {
            Oracle want = expected.get(windowStart);
            assertThat(want).as("no oracle for window %s", windowStart).isNotNull();

            assertThat(candle.getOpen()).as("open at %s", windowStart)
                    .isEqualByComparingTo(want.open);
            assertThat(candle.getClose()).as("close at %s", windowStart)
                    .isEqualByComparingTo(want.close);
            assertThat(candle.getHigh()).as("high at %s", windowStart)
                    .isEqualByComparingTo(want.high);
            assertThat(candle.getLow()).as("low at %s", windowStart)
                    .isEqualByComparingTo(want.low);
            assertThat(candle.getVolume()).as("volume at %s", windowStart)
                    .isEqualByComparingTo(want.volume);
            assertThat(candle.getQuoteVolume()).as("quoteVolume at %s", windowStart)
                    .isEqualByComparingTo(want.quoteVolume);
            assertThat(candle.getVwap()).as("vwap at %s", windowStart)
                    .isEqualByComparingTo(Decimals.divide(want.quoteVolume, want.volume));
            assertThat(candle.getBuyVolume()).as("buyVolume at %s", windowStart)
                    .isEqualByComparingTo(want.buyVolume);
            assertThat(candle.getSellVolume()).as("sellVolume at %s", windowStart)
                    .isEqualByComparingTo(want.sellVolume);
            assertThat(candle.getTradeCount()).as("tradeCount at %s", windowStart)
                    .isEqualTo(want.count);

            // Sanity properties that hold for any correct candle, whatever the numbers are.
            assertThat(candle.getHigh()).isGreaterThanOrEqualTo(candle.getLow());
            assertThat(candle.getVwap()).isBetween(candle.getLow(), candle.getHigh());
            assertThat(candle.getBuyVolume().add(candle.getSellVolume()))
                    .isEqualByComparingTo(candle.getVolume());
            assertThat(candle.getIsFinal()).isTrue();
            assertThat(candle.getHeader().getEventTime()).isEqualTo(candle.getWindowStart());
        });
    }

    @Test
    void finalisesEveryWindowTheFixtureCloses(@TempDir Path stateDir) {
        // The fixture spans 13:40 to 13:45. With a 30s grace, a window finalises only once a
        // later trade pushes stream time past windowEnd+grace, so the last window in the
        // capture is expected to stay open — that is the documented cost of driving
        // finalisation from stream time rather than a wall clock.
        Map<Instant, Candle> produced = finalCandles(fixture(), stateDir);

        assertThat(produced.keySet()).containsExactly(
                Instant.parse("2026-08-23T13:40:00Z"),
                Instant.parse("2026-08-23T13:41:00Z"),
                Instant.parse("2026-08-23T13:42:00Z"),
                Instant.parse("2026-08-23T13:43:00Z"));
    }

    /** Runs the fixture through a fresh driver and returns the finalised candles by window. */
    private static Map<Instant, Candle> finalCandles(List<RawEnvelope> frames, Path stateDir) {
        ProcessorConfig config = new ProcessorConfig(
                "localhost:9092", REGISTRY,
                "jdbc:none", "none", "none",
                "market-processor-replay", stateDir.toString(),
                0, Duration.ofMinutes(1), Duration.ofSeconds(30), Duration.ofMillis(0),
                1024 * 1024);

        Serde<String> keys = Serdes.String();
        Serde<RawEnvelope> raw = AvroSerdes.forValue(REGISTRY);
        Serde<Candle> candleSerde = AvroSerdes.forValue(REGISTRY);

        Map<Instant, Candle> finals = new LinkedHashMap<>();
        try (ProcessorMetrics metrics = new ProcessorMetrics();
                TopologyTestDriver driver = new TopologyTestDriver(
                        ProcessorTopology.build(config, "0.1.0-TEST", metrics, () -> PROCESSED_AT),
                        StreamsSettings.forProcessor(config))) {

            TestInputTopic<String, RawEnvelope> input = driver.createInputTopic(
                    Topics.RAW_KRAKEN_TRADE, keys.serializer(), raw.serializer());
            TestOutputTopic<String, Candle> output = driver.createOutputTopic(
                    Topics.DERIVED_CANDLES, keys.deserializer(), candleSerde.deserializer());

            frames.forEach(frame -> input.pipeInput(KEY, frame));

            output.readValuesToList().stream()
                    .filter(Candle::getIsFinal)
                    // A window is finalised once, so a duplicate here would be a bug rather
                    // than something to merge away.
                    .forEach(candle -> assertThat(finals.put(candle.getWindowStart(), candle))
                            .as("window %s was finalised twice", candle.getWindowStart())
                            .isNull());
        }
        return finals;
    }

    /** OHLCV computed straight from the fixture, deliberately sharing no code with the topology. */
    private record Oracle(
            BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close,
            BigDecimal volume, BigDecimal quoteVolume,
            BigDecimal buyVolume, BigDecimal sellVolume, int count) {
    }

    private static Map<Instant, Oracle> oracle(List<RawEnvelope> frames) {
        record Trade(Instant at, BigDecimal price, BigDecimal quantity, boolean buy) {
        }

        List<Trade> trades = new ArrayList<>();
        for (RawEnvelope frame : frames) {
            JsonNode root = readTree(frame);
            for (JsonNode node : root.get("data")) {
                trades.add(new Trade(
                        Instant.parse(node.get("timestamp").asText()),
                        node.get("price").decimalValue(),
                        node.get("qty").decimalValue(),
                        "buy".equals(node.get("side").asText())));
            }
        }

        Map<Instant, List<Trade>> byWindow = new LinkedHashMap<>();
        for (Trade trade : trades) {
            long minute = Math.floorDiv(trade.at().toEpochMilli(), 60_000L) * 60_000L;
            byWindow.computeIfAbsent(Instant.ofEpochMilli(minute), key -> new ArrayList<>())
                    .add(trade);
        }

        Map<Instant, Oracle> oracles = new LinkedHashMap<>();
        byWindow.forEach((windowStart, window) -> {
            BigDecimal high = window.get(0).price();
            BigDecimal low = window.get(0).price();
            BigDecimal volume = BigDecimal.ZERO;
            BigDecimal quoteVolume = BigDecimal.ZERO;
            BigDecimal buyVolume = BigDecimal.ZERO;
            BigDecimal sellVolume = BigDecimal.ZERO;
            for (Trade trade : window) {
                high = high.max(trade.price());
                low = low.min(trade.price());
                volume = volume.add(trade.quantity());
                quoteVolume = quoteVolume.add(trade.price().multiply(trade.quantity()));
                if (trade.buy()) {
                    buyVolume = buyVolume.add(trade.quantity());
                } else {
                    sellVolume = sellVolume.add(trade.quantity());
                }
            }
            oracles.put(windowStart, new Oracle(
                    window.get(0).price(), high, low, window.get(window.size() - 1).price(),
                    volume, quoteVolume, buyVolume, sellVolume, window.size()));
        });
        return oracles;
    }

    private static JsonNode readTree(RawEnvelope frame) {
        ByteBuffer buffer = frame.getPayload().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        try {
            return MAPPER.readTree(new String(bytes, StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<RawEnvelope> fixture() {
        List<RawEnvelope> frames = new ArrayList<>();
        try (InputStream stream = DeterministicReplayTest.class.getResourceAsStream(FIXTURE);
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {

            String line;
            long sequence = 0;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") || line.isBlank()) {
                    continue;
                }
                String[] columns = line.split("\t", 2);
                frames.add(envelope(Long.parseLong(columns[0]), columns[1], sequence++));
            }
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
        assertThat(frames).as("fixture " + FIXTURE).isNotEmpty();
        return frames;
    }

    private static RawEnvelope envelope(long receivedAtMillis, String payload, long sequence) {
        return RawEnvelope.newBuilder()
                // Derived from the frame's position rather than random, so that rebuilding
                // the fixture's envelopes is itself reproducible.
                .setEventId(UUID.nameUUIDFromBytes(("replay:" + sequence).getBytes(StandardCharsets.UTF_8)))
                .setExchange(Exchange.KRAKEN)
                .setChannel("trade")
                .setInstrument("BTC/USD")
                .setReceivedAt(Instant.ofEpochMilli(receivedAtMillis))
                .setSourceConnectionId("13834774380200032777")
                .setIngestSequence(sequence)
                .setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
                .setPayloadEncoding("json")
                .setTraceId("replay-" + sequence)
                .setSchemaVersion(1)
                .build();
    }
}
