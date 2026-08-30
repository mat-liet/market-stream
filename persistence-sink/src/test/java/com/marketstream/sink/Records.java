package com.marketstream.sink;

import com.marketstream.avro.Candle;
import com.marketstream.avro.CandleWindow;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.EventTimeSource;
import com.marketstream.avro.Exchange;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Canonical records for the tests, shaped like what the processor actually emits.
 *
 * <p>Values are deliberately awkward: a price with more precision than a {@code double} holds,
 * a quantity with trailing zeros, and a timestamp with non-zero millis. A fixture of round
 * numbers would pass against a mapping that silently truncated any of the three.
 */
final class Records {

    static final Instant EVENT_TIME = Instant.parse("2026-08-30T13:41:07.123Z");
    static final Instant WINDOW_START = Instant.parse("2026-08-30T13:41:00Z");
    static final UUID EVENT_ID = UUID.fromString("6f1a6a1e-6f2b-4a3c-9d4e-5f6a7b8c9d0e");

    private Records() {
    }

    static EventHeader header(Instant eventTime) {
        return EventHeader.newBuilder()
                .setEventId(EVENT_ID)
                .setExchange(Exchange.KRAKEN)
                .setInstrument("BTC/USD")
                .setEventTime(eventTime)
                .setIngestionTime(eventTime.plusMillis(37))
                .setProcessingTime(null)
                .setEventTimeSource(EventTimeSource.EXCHANGE)
                .setSchemaVersion(1)
                .setTraceId("trace-abc")
                .setSourceEventId(null)
                .build();
    }

    static TradeEvent trade() {
        return trade("70154386", Decimals.parse("60123.450000000001"));
    }

    static TradeEvent trade(String tradeId, BigDecimal price) {
        return TradeEvent.newBuilder()
                .setHeader(header(EVENT_TIME))
                .setTradeId(tradeId)
                .setPrice(price)
                .setQuantity(Decimals.parse("0.00100000"))
                .setSide(Side.BUY)
                .setDedupeKey("BTC/USD|" + tradeId)
                .build();
    }

    static Candle candle(boolean isFinal) {
        return Candle.newBuilder()
                .setHeader(header(WINDOW_START))
                .setWindow(CandleWindow.M1)
                .setWindowStart(WINDOW_START)
                .setWindowEnd(WINDOW_START.plusSeconds(60))
                .setOpen(Decimals.parse("60100.10"))
                .setHigh(Decimals.parse("60300.30"))
                .setLow(Decimals.parse("60000.05"))
                .setClose(Decimals.parse("60250.25"))
                .setVolume(Decimals.parse("2.5"))
                .setQuoteVolume(Decimals.parse("150375.625"))
                .setVwap(Decimals.parse("60150.25"))
                .setBuyVolume(Decimals.parse("1.5"))
                .setSellVolume(Decimals.parse("1.0"))
                .setTradeCount(42)
                .setIsFinal(isFinal)
                .setInputTradeCount(41)
                .setProcessorVersion("0.1.0-test")
                .build();
    }
}
