package com.marketstream.sink;

import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * {@code normalized.trades} → {@code market.trades}.
 *
 * <p>The only place in the system mapping a canonical trade onto columns. The order of
 * {@link #columns()} and the order of the {@code set*} calls in {@link #bind} are one fact
 * written twice, which is exactly the kind of thing that silently transposes two columns of
 * the same type; {@code TradesTableTest} pins them against each other.
 *
 * <p>ClickHouse dedupes on {@code ORDER BY (exchange, instrument, event_time, trade_id)}, so
 * a redelivered trade overwrites rather than duplicates. {@code eventId} is carried for
 * lineage but is not part of that key — it is deterministic per trade anyway, so it would add
 * nothing to the identity.
 */
public final class TradesTable implements ClickHouseTable<TradeEvent> {

    @Override
    public String tableName() {
        return "market.trades";
    }

    @Override
    public List<String> columns() {
        return List.of(
                "exchange",
                "instrument",
                "trade_id",
                "event_id",
                "event_time",
                "ingestion_time",
                "event_time_source",
                "price",
                "quantity",
                "side",
                "trace_id");
    }

    @Override
    public Verdict inspect(TradeEvent trade) {
        if (!ClickHouseTable.fitsDecimal(trade.getPrice())) {
            return new Verdict.Reject("price does not fit Decimal(38,18)");
        }
        if (!ClickHouseTable.fitsDecimal(trade.getQuantity())) {
            return new Verdict.Reject("quantity does not fit Decimal(38,18)");
        }
        if (!ClickHouseTable.fitsDateTime64(trade.getHeader().getEventTime().toEpochMilli())) {
            return new Verdict.Reject("eventTime is outside the DateTime64(3) range");
        }
        if (!ClickHouseTable.fitsDateTime64(trade.getHeader().getIngestionTime().toEpochMilli())) {
            return new Verdict.Reject("ingestionTime is outside the DateTime64(3) range");
        }
        // Part of the ordering key, so an empty one would merge unrelated trades into one row.
        if (trade.getTradeId() == null || trade.getTradeId().isBlank()) {
            return new Verdict.Reject("tradeId is empty, and it is part of the dedupe key");
        }
        return Verdict.STORE;
    }

    @Override
    public void bind(PreparedStatement statement, TradeEvent trade) throws SQLException {
        var header = trade.getHeader();
        statement.setString(1, header.getExchange().name());
        statement.setString(2, header.getInstrument());
        statement.setString(3, trade.getTradeId());
        // A java.util.UUID, not a String: the schema's uuid logical type generates one, and
        // the column is a native UUID. Stringifying it here would still work but would cost a
        // parse on every row.
        statement.setObject(4, header.getEventId());
        statement.setObject(5, ClickHouseTable.atUtc(header.getEventTime().toEpochMilli()));
        statement.setObject(6, ClickHouseTable.atUtc(header.getIngestionTime().toEpochMilli()));
        statement.setString(7, header.getEventTimeSource().name());
        statement.setBigDecimal(8, Decimals.canonical(trade.getPrice()));
        statement.setBigDecimal(9, Decimals.canonical(trade.getQuantity()));
        statement.setString(10, trade.getSide().name());
        statement.setString(11, header.getTraceId());
    }
}
