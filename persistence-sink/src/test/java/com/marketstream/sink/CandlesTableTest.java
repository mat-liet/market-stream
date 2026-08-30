package com.marketstream.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Candle;
import com.marketstream.common.Decimals;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class CandlesTableTest {

    private final CandlesTable table = new CandlesTable();

    @Test
    void bindsExactlyOneParameterPerColumn() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.candle(true));

        assertThat(bound.keySet())
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.rangeClosed(1, table.columns().size()).boxed().toList());
    }

    @Test
    void bindsEachPriceToItsOwnColumn() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.candle(true));

        // Nine same-typed decimals in a row: transposing two of them would be invisible in
        // every other test and permanent in the stored data.
        assertThat(bound.get(index("open"))).isEqualTo(Decimals.parse("60100.10"));
        assertThat(bound.get(index("high"))).isEqualTo(Decimals.parse("60300.30"));
        assertThat(bound.get(index("low"))).isEqualTo(Decimals.parse("60000.05"));
        assertThat(bound.get(index("close"))).isEqualTo(Decimals.parse("60250.25"));
        assertThat(bound.get(index("volume"))).isEqualTo(Decimals.parse("2.5"));
        assertThat(bound.get(index("quote_volume"))).isEqualTo(Decimals.parse("150375.625"));
        assertThat(bound.get(index("vwap"))).isEqualTo(Decimals.parse("60150.25"));
        assertThat(bound.get(index("buy_volume"))).isEqualTo(Decimals.parse("1.5"));
        assertThat(bound.get(index("sell_volume"))).isEqualTo(Decimals.parse("1.0"));
    }

    @Test
    void bindsWindowIdentityAndProvenance() {
        Map<Integer, Object> bound = BoundRow.bind(table, Records.candle(true));

        assertThat(bound.get(index("`window`"))).isEqualTo("M1");
        assertThat(bound.get(index("window_start"))).isEqualTo(LocalDateTime.parse("2026-08-30T13:41:00"));
        assertThat(bound.get(index("window_end"))).isEqualTo(LocalDateTime.parse("2026-08-30T13:42:00"));
        assertThat(bound.get(index("trade_count"))).isEqualTo(42);
        assertThat(bound.get(index("input_trade_count"))).isEqualTo(41);
        assertThat(bound.get(index("processor_version"))).isEqualTo("0.1.0-test");
    }

    @Test
    void quotesTheWindowColumnBecauseItCollidesWithClickHouseSyntax() {
        assertThat(table.insertSql()).contains("`window`");
    }

    @Test
    void storesAFinalCandle() {
        assertThat(table.inspect(Records.candle(true)))
                .isInstanceOf(ClickHouseTable.Verdict.Store.class);
    }

    @Test
    void skipsAProvisionalCandleRatherThanRejectingIt() {
        // market.candles has no is_final column, so a provisional row would be
        // indistinguishable from the final one it is about to be replaced by. Skipping is not
        // an error: the offset still advances.
        assertThat(table.inspect(Records.candle(false)))
                .isInstanceOfSatisfying(
                        ClickHouseTable.Verdict.Skip.class,
                        skip -> assertThat(skip.reason()).isEqualTo("provisional"));
    }

    @Test
    void rejectsADecimalTooLargeForTheColumn() {
        Candle oversized = Candle.newBuilder(Records.candle(true))
                .setQuoteVolume(Decimals.canonical(new BigDecimal("123456789012345678901")))
                .build();

        assertThat(table.inspect(oversized)).isInstanceOf(ClickHouseTable.Verdict.Reject.class);
    }

    @Test
    void rejectsANegativeCountBecauseTheColumnIsUnsigned() {
        // UInt32 would wrap a negative into four billion rather than fail, which is worse than
        // either storing it or dropping it.
        Candle negative = Candle.newBuilder(Records.candle(true)).setTradeCount(-1).build();

        assertThat(table.inspect(negative)).isInstanceOf(ClickHouseTable.Verdict.Reject.class);
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
