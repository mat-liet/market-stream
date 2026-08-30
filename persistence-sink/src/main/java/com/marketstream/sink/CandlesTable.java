package com.marketstream.sink;

import com.marketstream.avro.Candle;
import com.marketstream.common.Decimals;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * {@code derived.candles} → {@code market.candles}.
 *
 * <p><strong>Finals only.</strong> {@code market.candles} has no {@code is_final} column, and
 * the migration's own comment says the sink writes only {@code isFinal=true} rows in phase 1.
 * That settles what §9 leaves ambiguous when it says the final row overwrites the provisional
 * one: a provisional candle is never stored at all, so every row in the table is immutable by
 * construction (invariant 1) and a reader never has to ask whether the window it is looking at
 * is still moving. Provisional records are skipped, not rejected — their offsets advance
 * exactly as if they had been written.
 *
 * <p>ClickHouse dedupes on {@code ORDER BY (exchange, instrument, window, window_start)}, which
 * is the candle's natural identity, so a redelivery after a sink restart overwrites its own
 * earlier row rather than duplicating it.
 */
public final class CandlesTable implements ClickHouseTable<Candle> {

    @Override
    public String tableName() {
        return "market.candles";
    }

    @Override
    public List<String> columns() {
        return List.of(
                "exchange",
                "instrument",
                // Backticked: `window` collides with ClickHouse's windowing functions and an
                // unquoted one is a parse error, not a subtle misbinding.
                "`window`",
                "window_start",
                "window_end",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "vwap",
                "buy_volume",
                "sell_volume",
                "trade_count",
                "input_trade_count",
                "processor_version");
    }

    @Override
    public Verdict inspect(Candle candle) {
        if (!candle.getIsFinal()) {
            return new Verdict.Skip("provisional");
        }
        for (BigDecimal value : List.of(
                candle.getOpen(),
                candle.getHigh(),
                candle.getLow(),
                candle.getClose(),
                candle.getVolume(),
                candle.getQuoteVolume(),
                candle.getVwap(),
                candle.getBuyVolume(),
                candle.getSellVolume())) {
            if (!ClickHouseTable.fitsDecimal(value)) {
                return new Verdict.Reject("a decimal does not fit Decimal(38,18)");
            }
        }
        if (!ClickHouseTable.fitsDateTime64(candle.getWindowStart().toEpochMilli())) {
            return new Verdict.Reject("windowStart is outside the DateTime64(3) range");
        }
        if (!ClickHouseTable.fitsDateTime64(candle.getWindowEnd().toEpochMilli())) {
            return new Verdict.Reject("windowEnd is outside the DateTime64(3) range");
        }
        // Both columns are UInt32; a negative count would wrap into a billion rather than fail.
        if (candle.getTradeCount() < 0 || candle.getInputTradeCount() < 0) {
            return new Verdict.Reject("a trade count is negative and the column is unsigned");
        }
        return Verdict.STORE;
    }

    @Override
    public void bind(PreparedStatement statement, Candle candle) throws SQLException {
        var header = candle.getHeader();
        statement.setString(1, header.getExchange().name());
        statement.setString(2, header.getInstrument());
        statement.setString(3, candle.getWindow().name());
        statement.setObject(4, ClickHouseTable.atUtc(candle.getWindowStart().toEpochMilli()));
        statement.setObject(5, ClickHouseTable.atUtc(candle.getWindowEnd().toEpochMilli()));
        statement.setBigDecimal(6, Decimals.canonical(candle.getOpen()));
        statement.setBigDecimal(7, Decimals.canonical(candle.getHigh()));
        statement.setBigDecimal(8, Decimals.canonical(candle.getLow()));
        statement.setBigDecimal(9, Decimals.canonical(candle.getClose()));
        statement.setBigDecimal(10, Decimals.canonical(candle.getVolume()));
        statement.setBigDecimal(11, Decimals.canonical(candle.getQuoteVolume()));
        statement.setBigDecimal(12, Decimals.canonical(candle.getVwap()));
        statement.setBigDecimal(13, Decimals.canonical(candle.getBuyVolume()));
        statement.setBigDecimal(14, Decimals.canonical(candle.getSellVolume()));
        statement.setInt(15, candle.getTradeCount());
        statement.setInt(16, candle.getInputTradeCount());
        statement.setString(17, candle.getProcessorVersion());
    }
}
