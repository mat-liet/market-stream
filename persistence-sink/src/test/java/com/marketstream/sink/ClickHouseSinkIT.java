package com.marketstream.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.marketstream.avro.Candle;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Proves the two properties the whole milestone rests on, against a real ClickHouse.
 *
 * <p>First, that the values survive: a {@code Decimal(38,18)} keeps its scale, a
 * {@code DateTime64(3)} keeps its milliseconds, and a {@code UUID} stays the same UUID. Those
 * conversions live in the JDBC driver, so no unit test can vouch for them.
 *
 * <p>Second, and more important, that <strong>writing the same row twice leaves one row</strong>.
 * Every delivery claim in the design (§19.2's "at-least-once plus idempotent equals
 * effectively-once") is really a claim that {@code ReplacingMergeTree} collapses a redelivery —
 * and that is only true if the columns the sink inserts line up exactly with the table's
 * {@code ORDER BY}. A single mismatched key column would leave the sink silently accumulating
 * duplicates on every restart, and nothing else in the test suite would notice.
 *
 * <p>Runs against the {@code make up} stack and skips when it is down, for the reason recorded
 * on {@code RawFramePublisherIT}: Testcontainers cannot start these containers on this
 * machine's Docker Engine.
 */
class ClickHouseSinkIT {

    private static final String URL = envOrDefault("CLICKHOUSE_URL", "jdbc:ch://localhost:8123/market");
    private static final String HTTP = envOrDefault("CLICKHOUSE_HTTP", "http://localhost:8123");
    private static final String USER = envOrDefault("CLICKHOUSE_USER", "market");
    private static final String PASSWORD = envOrDefault("CLICKHOUSE_PASSWORD", "market");

    private static SinkConfig config;

    @BeforeAll
    static void requireTheLocalStack() {
        assumeTrue(clickHouseReachable(), "ClickHouse is not reachable at " + HTTP + " — run `make up`");
        config = new SinkConfig(
                "unused", "unused", URL, USER, PASSWORD, "it",
                0, 1_000, 1_000,
                Duration.ofSeconds(1), Duration.ofMillis(100),
                Duration.ofMillis(100), Duration.ofSeconds(1));
    }

    @Test
    void storesATradeWithItsDecimalsTimestampsAndIdIntact() throws Exception {
        // Unique per run: these tables also hold whatever the live pipeline has been writing.
        String instrument = "IT/" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant eventTime = Instant.parse("2026-08-30T13:41:07.123Z");
        BigDecimal price = Decimals.parse("60123.450000000001");

        TradeEvent trade = TradeEvent.newBuilder(Records.trade())
                .setHeader(EventHeader.newBuilder(Records.header(eventTime))
                        .setEventId(eventId)
                        .setInstrument(instrument)
                        .build())
                .setPrice(price)
                .build();

        try (ClickHouseWriter<TradeEvent> writer = new ClickHouseWriter<>(config, new TradesTable())) {
            writer.write(List.of(trade));
        }

        try (Connection connection = connect();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT price, quantity, event_time, event_id, side, written_at"
                                + " FROM market.trades FINAL WHERE instrument = ?")) {
            statement.setString(1, instrument);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).as("the trade was stored").isTrue();

                // Scale included: a Decimal(38,18) read back as 60123.45 would mean the column
                // or the driver had quietly rescaled it.
                assertThat(rows.getBigDecimal("price")).isEqualByComparingTo(price);
                assertThat(rows.getBigDecimal("price").scale()).isEqualTo(Decimals.SCALE);
                assertThat(rows.getBigDecimal("quantity")).isEqualByComparingTo(Decimals.parse("0.001"));

                // Read back as the literal value in the column rather than through
                // getTimestamp(), which would reinterpret it in the JVM's own timezone and
                // make this assertion pass or fail depending on where the developer is
                // sitting. What matters is that the wall-clock UTC value we bound is the
                // wall-clock value stored, milliseconds included.
                assertThat(rows.getObject("event_time", java.time.LocalDateTime.class))
                        .isEqualTo(ClickHouseTable.atUtc(eventTime.toEpochMilli()));

                assertThat(UUID.fromString(rows.getString("event_id"))).isEqualTo(eventId);
                assertThat(rows.getString("side")).isEqualTo("BUY");
                // Defaulted by the column, because the sink deliberately does not send it.
                assertThat(rows.getTimestamp("written_at")).isNotNull();

                assertThat(rows.next()).as("exactly one row").isFalse();
            }
        }
    }

    @Test
    void storesACandleAndCollapsesARedeliveryOfItIntoOneRow() throws Exception {
        String instrument = "IT/" + UUID.randomUUID();
        Candle candle = Candle.newBuilder(Records.candle(true))
                .setHeader(EventHeader.newBuilder(Records.header(Records.WINDOW_START))
                        .setInstrument(instrument)
                        .build())
                .build();

        try (ClickHouseWriter<Candle> writer = new ClickHouseWriter<>(config, new CandlesTable())) {
            writer.write(List.of(candle));
            // Exactly what a sink restart between the ClickHouse ack and the offset commit
            // does. The second write must overwrite the first, not sit beside it.
            writer.write(List.of(candle));
        }

        try (Connection connection = connect()) {
            assertThat(countOf(connection, "SELECT count() FROM market.candles FINAL WHERE instrument = ?",
                            instrument))
                    .as("ReplacingMergeTree collapses the redelivery on the natural key")
                    .isEqualTo(1L);

            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT `window`, window_start, window_end, open, high, low, close,"
                            + " volume, quote_volume, vwap, buy_volume, sell_volume,"
                            + " trade_count, input_trade_count, processor_version"
                            + " FROM market.candles FINAL WHERE instrument = ?")) {
                statement.setString(1, instrument);
                try (ResultSet rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getString("window")).isEqualTo("M1");
                    assertThat(rows.getObject("window_start", java.time.LocalDateTime.class))
                            .isEqualTo(ClickHouseTable.atUtc(Records.WINDOW_START.toEpochMilli()));
                    assertThat(rows.getObject("window_end", java.time.LocalDateTime.class))
                            .isEqualTo(ClickHouseTable.atUtc(
                                    Records.WINDOW_START.plusSeconds(60).toEpochMilli()));
                    // Each read back by name, so a transposition in the column list fails here
                    // rather than living forever in the stored data.
                    assertThat(rows.getBigDecimal("open")).isEqualByComparingTo("60100.10");
                    assertThat(rows.getBigDecimal("high")).isEqualByComparingTo("60300.30");
                    assertThat(rows.getBigDecimal("low")).isEqualByComparingTo("60000.05");
                    assertThat(rows.getBigDecimal("close")).isEqualByComparingTo("60250.25");
                    assertThat(rows.getBigDecimal("volume")).isEqualByComparingTo("2.5");
                    assertThat(rows.getBigDecimal("quote_volume")).isEqualByComparingTo("150375.625");
                    assertThat(rows.getBigDecimal("vwap")).isEqualByComparingTo("60150.25");
                    assertThat(rows.getBigDecimal("buy_volume")).isEqualByComparingTo("1.5");
                    assertThat(rows.getBigDecimal("sell_volume")).isEqualByComparingTo("1.0");
                    assertThat(rows.getInt("trade_count")).isEqualTo(42);
                    assertThat(rows.getInt("input_trade_count")).isEqualTo(41);
                    assertThat(rows.getString("processor_version")).isEqualTo("0.1.0-test");
                }
            }
        }
    }

    @Test
    void writesABatchOfManyCandlesInOneInsert() throws Exception {
        String instrument = "IT/" + UUID.randomUUID();
        List<Candle> minutes = java.util.stream.IntStream.range(0, 250)
                .mapToObj(minute -> Candle.newBuilder(Records.candle(true))
                        .setHeader(EventHeader.newBuilder(Records.header(Records.WINDOW_START))
                                .setInstrument(instrument)
                                .build())
                        .setWindowStart(Records.WINDOW_START.plusSeconds(60L * minute))
                        .setWindowEnd(Records.WINDOW_START.plusSeconds(60L * (minute + 1)))
                        .build())
                .toList();

        try (ClickHouseWriter<Candle> writer = new ClickHouseWriter<>(config, new CandlesTable())) {
            writer.write(minutes);
        }

        try (Connection connection = connect()) {
            assertThat(countOf(connection,
                            "SELECT count() FROM market.candles FINAL WHERE instrument = ?", instrument))
                    .isEqualTo(250L);
        }
    }

    /**
     * Removes what the tests wrote.
     *
     * <p>These are the same tables the live pipeline fills and M5's API will serve, so leaving
     * a few hundred synthetic candles behind would put fabricated data in front of anyone
     * looking at the demo. The {@code IT/} instrument prefix is what makes the cleanup exact.
     *
     * <p>Mutations are asynchronous, so this schedules the delete rather than waiting for it;
     * nothing in these tests depends on the rows being gone.
     */
    @AfterAll
    static void removeWhatTheseTestsWrote() throws SQLException {
        if (config == null) {
            return;
        }
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE market.trades DELETE WHERE instrument LIKE 'IT/%'");
            statement.execute("ALTER TABLE market.candles DELETE WHERE instrument LIKE 'IT/%'");
        }
    }

    private static long countOf(Connection connection, String sql, String instrument) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, instrument);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    private static Connection connect() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        return DriverManager.getConnection(URL, properties);
    }

    private static boolean clickHouseReachable() {
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(HTTP + "/ping"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
