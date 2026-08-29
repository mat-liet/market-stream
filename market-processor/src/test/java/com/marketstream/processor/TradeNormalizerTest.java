package com.marketstream.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Exchange;
import com.marketstream.avro.EventTimeSource;
import com.marketstream.avro.InvalidReason;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TradeNormalizerTest {

    private static final Instant RECEIVED_AT = Instant.parse("2026-08-23T12:00:00.500Z");

    private final TradeNormalizer normalizer = new TradeNormalizer("0.1.0-TEST");

    @Test
    void normalizesATradeUpdateIntoACanonicalTradeEvent() {
        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"sell","price":60000.1,"qty":0.001,
                   "ord_type":"market","trade_id":70154386,"timestamp":"2026-08-23T12:00:00.123456Z"}]}
                """));

        assertThat(results).hasSize(1);
        TradeEvent trade = normalized(results.get(0));

        assertThat(trade.getTradeId()).isEqualTo("70154386");
        assertThat(trade.getSide()).isEqualTo(Side.SELL);
        assertThat(trade.getPrice()).isEqualByComparingTo("60000.1");
        assertThat(trade.getQuantity()).isEqualByComparingTo("0.001");
        assertThat(trade.getDedupeKey()).isEqualTo("BTC/USD|70154386");
        assertThat(trade.getHeader().getExchange()).isEqualTo(Exchange.KRAKEN);
        assertThat(trade.getHeader().getInstrument()).isEqualTo("BTC/USD");
        assertThat(trade.getHeader().getEventTimeSource()).isEqualTo(EventTimeSource.EXCHANGE);
        // Left null on purpose: a wall clock here would be the only thing making
        // normalized.trades differ between two replays of the same input.
        assertThat(trade.getHeader().getProcessingTime()).isNull();
    }

    @Test
    void keysEveryTradeByExchangeAndInstrument() {
        List<NormalizationResult> results = normalizer.normalize(null, envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"eth/usd","side":"buy","price":2600.5,"qty":1.2,
                   "trade_id":1,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """));

        // The key comes from the payload, which is authoritative, not from the incoming
        // record key — and it is upper-cased on the way through.
        assertThat(results.get(0).key()).isEqualTo("KRAKEN|ETH/USD");
    }

    @Test
    void keepsPriceAndQuantityExactRatherThanRoundingThroughDouble() {
        // The entire decimal guarantee rests on this. Kraken sends price and qty as JSON
        // floats; if Jackson parses them into a double first, the exact value is gone before
        // normalisation ever sees it and every VWAP downstream inherits the drift.
        // 60123.450000000001 is a real price captured off the live feed in M2.
        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy",
                   "price":12345.678901234567890123,"qty":60123.450000000001,
                   "trade_id":1,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """));

        TradeEvent trade = normalized(results.get(0));
        assertThat(trade.getPrice()).isEqualByComparingTo(new BigDecimal("12345.678901234567890123"));
        assertThat(trade.getQuantity()).isEqualByComparingTo(new BigDecimal("60123.450000000001"));
    }

    @Test
    void fansOneFrameWithSeveralTradesOutIntoSeveralEvents() {
        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "trade_id":1,"timestamp":"2026-08-23T12:00:00.000000Z"},
                  {"symbol":"BTC/USD","side":"sell","price":60001.0,"qty":2.0,
                   "trade_id":2,"timestamp":"2026-08-23T12:00:00.100000Z"}]}
                """));

        assertThat(results).hasSize(2);
        assertThat(results).extracting(result -> normalized(result).getTradeId())
                .containsExactly("1", "2");
    }

    @Test
    void truncatesMicrosecondTimestampsToTheSchemasMillisecondPrecision() {
        // Kraken sends microseconds but eventTime is timestamp-millis. Truncating here
        // rather than at the serialiser keeps the in-memory record equal to its round trip.
        TradeEvent trade = normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "trade_id":1,"timestamp":"2026-08-23T12:00:00.123456Z"}]}
                """)).get(0));

        assertThat(trade.getHeader().getEventTime()).isEqualTo(Instant.parse("2026-08-23T12:00:00.123Z"));
    }

    @Test
    void fallsBackToIngestionTimeWhenTheExchangeTimestampIsMissing() {
        TradeEvent trade = normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,"trade_id":1}]}
                """)).get(0));

        assertThat(trade.getHeader().getEventTime()).isEqualTo(RECEIVED_AT);
        assertThat(trade.getHeader().getEventTimeSource())
                .isEqualTo(EventTimeSource.INGESTION_FALLBACK);
    }

    @Test
    void fallsBackToIngestionTimeWhenTheExchangeTimestampIsAbsurd() {
        // A timestamp years in the future would advance stream time past every open window
        // and close them all prematurely. EventTimeResolver bounds that blast radius.
        TradeEvent trade = normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "trade_id":1,"timestamp":"2099-01-01T00:00:00.000000Z"}]}
                """)).get(0));

        assertThat(trade.getHeader().getEventTime()).isEqualTo(RECEIVED_AT);
        assertThat(trade.getHeader().getEventTimeSource())
                .isEqualTo(EventTimeSource.INGESTION_FALLBACK);
    }

    @Test
    void routesMalformedJsonToDeadLetterWithItsEnvelopeIntact() {
        RawEnvelope envelope = envelope("{\"channel\":\"trade\",\"data\":[");

        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope);

        assertThat(results).singleElement()
                .isInstanceOfSatisfying(NormalizationResult.Unparseable.class, unparseable -> {
                    // Forwarded verbatim: dead-letter's registry subject is bound to
                    // RawEnvelope by the ingestor, and the connection id and ingest sequence
                    // are what make the bad frame findable again.
                    assertThat(unparseable.envelope()).isSameAs(envelope);
                    assertThat(unparseable.key()).isEqualTo("KRAKEN|BTC/USD");
                });
    }

    @Test
    void routesAnUnknownChannelToInvalidEventsRatherThanDeadLetter() {
        // We understood it well enough to say what was wrong, which is the whole distinction
        // between the two ops topics (design doc 11.7).
        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":60000.0}]}
                """));

        assertThat(results).singleElement()
                .isInstanceOfSatisfying(NormalizationResult.Rejected.class, rejected ->
                        assertThat(rejected.invalid().getReason()).isEqualTo(InvalidReason.UNKNOWN_TYPE));
    }

    @Test
    void rejectsATradeWithNoSizeBecauseItWouldSkewVwapWeighting() {
        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":0.0,
                   "trade_id":1,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """));

        assertThat(results).singleElement()
                .isInstanceOfSatisfying(NormalizationResult.Rejected.class, rejected -> {
                    assertThat(rejected.invalid().getReason()).isEqualTo(InvalidReason.IMPOSSIBLE_STATE);
                    assertThat(rejected.invalid().getInstrument()).isEqualTo("BTC/USD");
                    assertThat(rejected.invalid().getProcessorVersion()).isEqualTo("0.1.0-TEST");
                });
    }

    @Test
    void buildsADeterministicCompositeIdWhenTheExchangeSendsNoTradeId() {
        // Kraken v2 does carry trade_id, so this is the fallback path. The ordinal is what
        // separates two trades that share a millisecond, price, size and side — needed
        // because the sub-millisecond digits are gone by this point.
        String frame = """
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "timestamp":"2026-08-23T12:00:00.000000Z"},
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """;

        List<NormalizationResult> results = normalizer.normalize("KRAKEN|BTC/USD", envelope(frame));

        assertThat(results).extracting(result -> normalized(result).getTradeId())
                .containsExactly(
                        "c:BTC/USD:1787486400000:60000.000000000000000000:1.000000000000000000:BUY:0",
                        "c:BTC/USD:1787486400000:60000.000000000000000000:1.000000000000000000:BUY:1");
    }

    @Test
    void buildsTheSameCompositeIdHoweverTheExchangeWroteTheNumber() {
        // Jackson normalises trailing zeros as it parses, so 60000 and 60000.00 arrive as
        // different BigDecimals. Composing the id from those renderings would give one trade
        // two identities — the exact failure the composite key exists to prevent.
        assertThat(compositeIdFor("60000", "1")).isEqualTo(compositeIdFor("60000.00", "1.000"));
    }

    private String compositeIdFor(String price, String quantity) {
        return normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":%s,"qty":%s,
                   "timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """.formatted(price, quantity))).get(0)).getTradeId();
    }

    @Test
    void givesTheSameTradeTheSameEventIdEveryTimeItIsNormalized() {
        // Correctness invariant 6: replaying identical input must produce identical output.
        // A random UUID here would break that on the very first field.
        String frame = """
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"buy","price":60000.0,"qty":1.0,
                   "trade_id":7,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """;

        UUID first = normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope(frame)).get(0))
                .getHeader().getEventId();
        UUID second = normalized(normalizer.normalize("KRAKEN|BTC/USD", envelope(frame)).get(0))
                .getHeader().getEventId();

        assertThat(first).isEqualTo(second);
    }

    private static TradeEvent normalized(NormalizationResult result) {
        assertThat(result).isInstanceOf(NormalizationResult.Normalized.class);
        return ((NormalizationResult.Normalized) result).trade();
    }

    private static RawEnvelope envelope(String payload) {
        return RawEnvelope.newBuilder()
                .setEventId(UUID.fromString("00000000-0000-0000-0000-0000000000ff"))
                .setExchange(Exchange.KRAKEN)
                .setChannel("trade")
                .setInstrument("BTC/USD")
                .setReceivedAt(RECEIVED_AT)
                .setSourceConnectionId("14134529360570871696")
                .setIngestSequence(42L)
                .setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
                .setPayloadEncoding("json")
                .setTraceId("trace-1")
                .setSchemaVersion(1)
                .build();
    }
}
