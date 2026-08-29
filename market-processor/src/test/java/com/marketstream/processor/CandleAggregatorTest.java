package com.marketstream.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Candle;
import com.marketstream.avro.CandleWindow;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.EventTimeSource;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CandleAggregatorTest {

    private static final Instant WINDOW_START = Instant.parse("2026-08-23T12:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-08-23T12:01:00Z");
    private static final Instant PROCESSED_AT = Instant.parse("2026-08-23T12:01:30Z");

    @Test
    void takesOpenFromTheFirstTradeAndCloseFromTheLast() {
        Candle candle = fold("100", "1", Side.BUY, "150", "1", Side.SELL, "120", "1", Side.BUY);

        assertThat(candle.getOpen()).isEqualByComparingTo("100");
        assertThat(candle.getClose()).isEqualByComparingTo("120");
        assertThat(candle.getHigh()).isEqualByComparingTo("150");
        assertThat(candle.getLow()).isEqualByComparingTo("100");
        assertThat(candle.getTradeCount()).isEqualTo(3);
        assertThat(candle.getInputTradeCount()).isEqualTo(3);
    }

    @Test
    void findsHighAndLowWhateverOrderTheTradesArriveIn() {
        // High and low must not depend on arrival order, or the aggregate stops being
        // reproducible after a rebalance replays a partition's tail.
        Candle ascending = fold("100", "1", Side.BUY, "120", "1", Side.BUY, "150", "1", Side.BUY);
        Candle descending = fold("150", "1", Side.BUY, "120", "1", Side.BUY, "100", "1", Side.BUY);

        assertThat(ascending.getHigh()).isEqualByComparingTo(descending.getHigh());
        assertThat(ascending.getLow()).isEqualByComparingTo(descending.getLow());
    }

    @Test
    void collapsesToASinglePriceForAOneTradeWindow() {
        Candle candle = stamp(fold("60000.5", "0.25", Side.BUY), true);

        assertThat(candle.getOpen()).isEqualByComparingTo("60000.5");
        assertThat(candle.getHigh()).isEqualByComparingTo("60000.5");
        assertThat(candle.getLow()).isEqualByComparingTo("60000.5");
        assertThat(candle.getClose()).isEqualByComparingTo("60000.5");
        assertThat(candle.getVolume()).isEqualByComparingTo("0.25");
        assertThat(candle.getVwap()).isEqualByComparingTo("60000.5");
    }

    @Test
    void weightsVwapByVolumeRatherThanAveragingPrices() {
        // The distinction that matters: the arithmetic mean of 100 and 200 is 150, but a
        // 1-unit trade at 100 and a 3-unit trade at 200 have a VWAP of 175.
        Candle candle = stamp(fold("100", "1", Side.BUY, "200", "3", Side.BUY), true);

        assertThat(candle.getQuoteVolume()).isEqualByComparingTo("700");
        assertThat(candle.getVolume()).isEqualByComparingTo("4");
        assertThat(candle.getVwap()).isEqualByComparingTo("175");
    }

    @Test
    void keepsVwapExactWhereFloatingPointWouldDrift() {
        // 0.1 + 0.2 is the canonical double-precision failure. Through BigDecimal the
        // numerator is exactly 0.3 and the VWAP is exactly 1.
        Candle candle = stamp(fold("1", "0.1", Side.BUY, "1", "0.2", Side.BUY), true);

        assertThat(candle.getVolume()).isEqualByComparingTo("0.3");
        assertThat(candle.getVwap()).isEqualByComparingTo("1");
    }

    @Test
    void reportsZeroVwapForAnUntouchedWindowRatherThanDividingByZero() {
        // An empty accumulator must be stampable: a window with no volume has no meaningful
        // VWAP, and throwing here would take down the emit path mid-aggregation.
        Candle candle = stamp(CandleAggregator.empty(), false);

        assertThat(candle.getVwap()).isEqualByComparingTo(Decimals.ZERO);
        assertThat(candle.getTradeCount()).isZero();
    }

    @Test
    void splitsVolumeByAggressorSide() {
        // The two sides deliberately sum to different totals, so a swap would fail rather
        // than pass by coincidence.
        Candle candle = fold("100", "2", Side.BUY, "100", "5", Side.SELL, "100", "1", Side.BUY);

        assertThat(candle.getBuyVolume()).isEqualByComparingTo("3");
        assertThat(candle.getSellVolume()).isEqualByComparingTo("5");
        assertThat(candle.getVolume()).isEqualByComparingTo("8");
    }

    @Test
    void stampsWindowCoordinatesFromTheWindowRatherThanTheTrades() {
        Candle candle = stamp(fold("100", "1", Side.BUY), true);

        assertThat(candle.getWindow()).isEqualTo(CandleWindow.M1);
        assertThat(candle.getWindowStart()).isEqualTo(WINDOW_START);
        assertThat(candle.getWindowEnd()).isEqualTo(WINDOW_END);
        // The schema fixes this: a candle's event time is the start of its window.
        assertThat(candle.getHeader().getEventTime()).isEqualTo(WINDOW_START);
        assertThat(candle.getHeader().getInstrument()).isEqualTo("BTC/USD");
        assertThat(candle.getIsFinal()).isTrue();
        assertThat(candle.getProcessorVersion()).isEqualTo("0.1.0-TEST");
    }

    @Test
    void givesProvisionalAndFinalCandlesDistinctEventIdsForTheSameWindow() {
        // They are two different records on one topic. Sharing an id would make them
        // indistinguishable to anything deduplicating by it — including the phase 1 sink.
        Candle accumulator = fold("100", "1", Side.BUY);

        assertThat(stamp(accumulator, false).getHeader().getEventId())
                .isNotEqualTo(stamp(accumulator, true).getHeader().getEventId());
    }

    @Test
    void givesTheSameWindowTheSameEventIdOnEveryReplay() {
        // Correctness invariant 6, at the candle level.
        assertThat(stamp(fold("100", "1", Side.BUY), true).getHeader().getEventId())
                .isEqualTo(stamp(fold("100", "1", Side.BUY), true).getHeader().getEventId());
    }

    /** Folds trades given as flat (price, quantity, side) triples. */
    private static Candle fold(Object... priceQuantitySideTriples) {
        Candle candle = CandleAggregator.empty();
        for (int i = 0; i < priceQuantitySideTriples.length; i += 3) {
            candle = CandleAggregator.fold(candle, trade(
                    (String) priceQuantitySideTriples[i],
                    (String) priceQuantitySideTriples[i + 1],
                    (Side) priceQuantitySideTriples[i + 2]));
        }
        return candle;
    }

    private static Candle stamp(Candle accumulator, boolean isFinal) {
        return CandleAggregator.stamp(
                accumulator, "KRAKEN|BTC/USD", WINDOW_START, WINDOW_END,
                isFinal, "0.1.0-TEST", PROCESSED_AT);
    }

    private static TradeEvent trade(String price, String quantity, Side side) {
        EventHeader header = EventHeader.newBuilder()
                .setEventId(UUID.nameUUIDFromBytes((price + quantity + side).getBytes()))
                .setExchange(Exchange.KRAKEN)
                .setInstrument("BTC/USD")
                .setEventTime(WINDOW_START)
                .setIngestionTime(WINDOW_START)
                .setProcessingTime(null)
                .setEventTimeSource(EventTimeSource.EXCHANGE)
                .setSchemaVersion(1)
                .setTraceId("trace-1")
                .setSourceEventId(null)
                .build();

        return TradeEvent.newBuilder()
                .setHeader(header)
                .setTradeId(price + '-' + quantity)
                .setPrice(Decimals.parse(price))
                .setQuantity(Decimals.parse(quantity))
                .setSide(side)
                .setDedupeKey("BTC/USD|" + price + '-' + quantity)
                .build();
    }
}
