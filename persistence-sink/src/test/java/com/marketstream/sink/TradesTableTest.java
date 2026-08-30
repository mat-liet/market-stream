package com.marketstream.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TradesTableTest {

    private final TradesTable table = new TradesTable();

    @Test
    void bindsExactlyOneParameterPerColumn() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.trade());

        assertThat(bound.keySet())
                .containsExactlyInAnyOrderElementsOf(
                        java.util.stream.IntStream.rangeClosed(1, table.columns().size())
                                .boxed()
                                .toList());
    }

    @Test
    void bindsEachValueAtItsColumnPosition() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.trade());

        assertThat(bound.get(index("exchange"))).isEqualTo("KRAKEN");
        assertThat(bound.get(index("instrument"))).isEqualTo("BTC/USD");
        assertThat(bound.get(index("trade_id"))).isEqualTo("70154386");
        assertThat(bound.get(index("event_time_source"))).isEqualTo("EXCHANGE");
        assertThat(bound.get(index("side"))).isEqualTo("BUY");
        assertThat(bound.get(index("trace_id"))).isEqualTo("trace-abc");
    }

    @Test
    void bindsTheEventIdAsAUuidRatherThanItsText() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.trade());

        // The column is a native UUID and the schema's logical type already produces one.
        // Passing the string form would work but would cost a parse on every row.
        assertThat(bound.get(index("event_id"))).isInstanceOf(UUID.class).isEqualTo(Records.EVENT_ID);
    }

    @Test
    void bindsTimestampsAsUtcLocalDateTimesSoTheServerCannotReinterpretThem() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.trade());

        // 2026-08-30T13:41:07.123Z, with the millis intact: DateTime64(3) keeps them, and a
        // conversion through java.sql.Timestamp or a bare Instant is where a timezone shift
        // would creep in.
        assertThat(bound.get(index("event_time")))
                .isEqualTo(LocalDateTime.parse("2026-08-30T13:41:07.123"));
        assertThat(bound.get(index("ingestion_time")))
                .isEqualTo(LocalDateTime.parse("2026-08-30T13:41:07.160"));
    }

    @Test
    void keepsALongTailPriceExactlyAsItArrived() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.trade());

        // The value that would not survive a double anywhere in the chain.
        assertThat(bound.get(index("price")))
                .isEqualTo(new BigDecimal("60123.450000000001000000"))
                .satisfies(value -> assertThat(((BigDecimal) value).scale()).isEqualTo(Decimals.SCALE));
    }

    @Test
    void storesAnOrdinaryTrade() {
        assertThat(table.inspect(Records.trade())).isInstanceOf(ClickHouseTable.Verdict.Store.class);
    }

    @Test
    void rejectsAPriceTooLargeForTheColumnRatherThanLettingTheInsertFail() {
        // 21 integer digits plus 18 fractional ones is 39, one past Decimal(38,18). Left to
        // the INSERT this would fail forever, and the retry loop would block the pipeline.
        TradeEvent oversized =
                Records.trade("1", Decimals.canonical(new BigDecimal("123456789012345678901")));

        assertThat(table.inspect(oversized))
                .isInstanceOfSatisfying(
                        ClickHouseTable.Verdict.Reject.class,
                        reject -> assertThat(reject.reason()).contains("price"));
    }

    @Test
    void rejectsATradeWithoutAnIdBecauseItIsPartOfTheDedupeKey() {
        TradeEvent anonymous = Records.trade("", Decimals.parse("1"));

        assertThat(table.inspect(anonymous))
                .isInstanceOfSatisfying(
                        ClickHouseTable.Verdict.Reject.class,
                        reject -> assertThat(reject.reason()).contains("tradeId"));
    }

    @Test
    void leavesWrittenAtToClickHouseSoAReplayOutranksTheRowItReplaces() {
        assertThat(table.columns()).doesNotContain("written_at");
        assertThat(table.insertSql()).doesNotContain("written_at");
    }

    private int index(String column) {
        return table.columns().indexOf(column) + 1;
    }
}
