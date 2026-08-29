package com.marketstream.processor;

import com.marketstream.avro.Candle;
import com.marketstream.avro.CandleWindow;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import com.marketstream.common.InstrumentKey;
import com.marketstream.schemas.SchemaVersions;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Folds trades into an OHLCV candle and stamps the finished record (design doc 12.7, 13.1).
 *
 * <p>Pure functions over {@link Candle}. Using the output schema as the accumulator avoids
 * inventing an internal type just to hold six running totals, and gets the changelog a
 * registry-managed schema for free.
 *
 * <p>The accumulator deliberately leaves everything identity-shaped — {@code windowStart},
 * {@code windowEnd}, the header — as placeholders, because it cannot see the window it is
 * folding into. Those are stamped by {@link #stamp} from the authoritative {@code Windowed}
 * key at emit time. Re-deriving window boundaries here from the trade's own timestamp would
 * work today and silently disagree with the topology the moment the window config changes.
 *
 * <p>Order-independence matters more than it looks. Trades within a window arrive in offset
 * order, but a fold that depended on arrival order for anything except {@code open} and
 * {@code close} would make the aggregate non-reproducible after a rebalance.
 */
public final class CandleAggregator {

    private CandleAggregator() {
    }

    /**
     * An empty accumulator. Every decimal is canonical zero, so the first fold never has to
     * special-case scale, and {@code tradeCount == 0} is what marks the candle as untouched.
     */
    public static Candle empty() {
        return Candle.newBuilder()
                .setHeader(placeholderHeader())
                .setWindow(CandleWindow.M1)
                .setWindowStart(Instant.EPOCH)
                .setWindowEnd(Instant.EPOCH)
                .setOpen(Decimals.ZERO)
                .setHigh(Decimals.ZERO)
                .setLow(Decimals.ZERO)
                .setClose(Decimals.ZERO)
                .setVolume(Decimals.ZERO)
                .setQuoteVolume(Decimals.ZERO)
                .setVwap(Decimals.ZERO)
                .setBuyVolume(Decimals.ZERO)
                .setSellVolume(Decimals.ZERO)
                .setTradeCount(0)
                .setIsFinal(false)
                .setInputTradeCount(0)
                .setProcessorVersion("")
                .build();
    }

    /** Folds one trade into the accumulator, returning a new value rather than mutating. */
    public static Candle fold(Candle accumulator, TradeEvent trade) {
        BigDecimal price = trade.getPrice();
        BigDecimal quantity = trade.getQuantity();
        boolean first = accumulator.getTradeCount() == 0;

        BigDecimal volume = accumulator.getVolume().add(quantity);
        // The VWAP numerator, accumulated rather than divided per trade: dividing here would
        // round every trade and compound the error across a busy minute.
        //
        // The product has scale 36 and is stored back at 18, so in principle this rounds.
        // In practice it does not: Kraken quotes prices to ~5 decimal places and sizes to
        // ~8, and 13 decimal places fit inside 18 exactly. Only an input with far more
        // precision than any real exchange sends would lose a digit here.
        BigDecimal quoteVolume = accumulator.getQuoteVolume().add(price.multiply(quantity));

        Candle.Builder builder = Candle.newBuilder(accumulator)
                // Carried on the accumulator so the emitted candle can be walked back to the
                // frame that closed it. Replaced wholesale by stamp(), like the rest of the
                // header, but this is the one part of it the fold actually knows.
                .setHeader(EventHeader.newBuilder(accumulator.getHeader())
                        .setInstrument(trade.getHeader().getInstrument())
                        .setTraceId(trade.getHeader().getTraceId())
                        .build())
                .setHigh(first ? price : accumulator.getHigh().max(price))
                .setLow(first ? price : accumulator.getLow().min(price))
                // Close follows arrival order, which within a window is offset order — the
                // last trade folded is the last trade of the window.
                .setClose(price)
                .setVolume(Decimals.canonical(volume))
                .setQuoteVolume(Decimals.canonical(quoteVolume))
                .setTradeCount(accumulator.getTradeCount() + 1)
                // Identical to tradeCount until the phase 2 dedupe store lands, at which
                // point this stays the pre-dedupe count and tradeCount becomes post-dedupe.
                .setInputTradeCount(accumulator.getInputTradeCount() + 1);

        if (first) {
            builder.setOpen(price);
        }
        if (trade.getSide() == Side.BUY) {
            builder.setBuyVolume(Decimals.canonical(accumulator.getBuyVolume().add(quantity)));
        } else {
            builder.setSellVolume(Decimals.canonical(accumulator.getSellVolume().add(quantity)));
        }
        return builder.build();
    }

    /**
     * Stamps a folded accumulator with its window coordinates and identity, producing the
     * record that goes on {@code derived.candles}.
     *
     * @param instrumentKey  the {@code exchange|instrument} Kafka key of the window
     * @param isFinal        false while the window is open, true once emitted past grace
     * @param processingTime the wall clock, the one field that legitimately differs between
     *                       two replays of the same input
     */
    public static Candle stamp(
            Candle accumulator,
            String instrumentKey,
            Instant windowStart,
            Instant windowEnd,
            boolean isFinal,
            String processorVersion,
            Instant processingTime) {

        InstrumentKey key = InstrumentKey.parse(instrumentKey);

        EventHeader header = EventHeader.newBuilder()
                .setEventId(DeterministicIds.forCandle(
                        instrumentKey, CandleWindow.M1.name(), windowStart.toEpochMilli(), isFinal))
                .setExchange(Exchange.valueOf(key.exchange().name()))
                .setInstrument(key.instrument())
                // The schema fixes this: a candle's event time is the start of its window.
                .setEventTime(windowStart)
                // A candle aggregates many frames, so there is no single ingestion time.
                // The window start is the closest honest answer and keeps the field's
                // "never later than processingTime" ordering intact.
                .setIngestionTime(windowStart)
                .setProcessingTime(processingTime)
                .setEventTimeSource(com.marketstream.avro.EventTimeSource.EXCHANGE)
                .setSchemaVersion(SchemaVersions.CURRENT)
                .setTraceId(accumulator.getHeader().getTraceId())
                // Lineage is to many source events, not one, so this stays null rather than
                // naming an arbitrary member of the window.
                .setSourceEventId(null)
                .build();

        return Candle.newBuilder(accumulator)
                .setHeader(header)
                .setWindow(CandleWindow.M1)
                .setWindowStart(windowStart)
                .setWindowEnd(windowEnd)
                // Computed once, at the end, from the exact numerator and denominator.
                // Decimals.divide returns zero for an empty window rather than throwing.
                .setVwap(Decimals.divide(accumulator.getQuoteVolume(), accumulator.getVolume()))
                .setIsFinal(isFinal)
                .setProcessorVersion(processorVersion)
                .build();
    }

    /**
     * The accumulator's header is never emitted — {@link #stamp} replaces it wholesale — but
     * it has to be a valid record because the changelog serialises it against a schema whose
     * header fields are all required.
     */
    private static EventHeader placeholderHeader() {
        return EventHeader.newBuilder()
                .setEventId(new java.util.UUID(0L, 0L))
                .setExchange(Exchange.KRAKEN)
                .setInstrument("")
                .setEventTime(Instant.EPOCH)
                .setIngestionTime(Instant.EPOCH)
                .setProcessingTime(null)
                .setEventTimeSource(com.marketstream.avro.EventTimeSource.EXCHANGE)
                .setSchemaVersion(SchemaVersions.CURRENT)
                .setTraceId("")
                .setSourceEventId(null)
                .build();
    }
}
