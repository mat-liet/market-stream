package com.marketstream.ingestor;

import com.marketstream.common.Exchange;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which markets to subscribe to, read from the Postgres config store (design doc 15.3).
 *
 * <p>Read once at startup and never again: a live registry change is a restart, not a
 * hot-reload. Subscriptions are connection state, so applying a change mid-stream would
 * mean tearing the connection down anyway.
 *
 * <p>Plain JDBC. One query against one table does not justify a connection pool or an ORM
 * in a service whose entire job is to move bytes.
 */
public final class InstrumentRegistry {

    /**
     * @param canonicalSymbol what the platform calls it ({@code BTC/USD}) — the instrument
     *                        half of every {@link com.marketstream.common.InstrumentKey}
     * @param exchangeSymbol  what the exchange calls it on the wire. Identical to the
     *                        canonical form for Kraken today, deliberately not assumed so.
     */
    public record Instrument(Exchange exchange, String canonicalSymbol, String exchangeSymbol) {
    }

    private static final String QUERY = """
            SELECT canonical_symbol, exchange_symbol
              FROM instrument
             WHERE exchange = ? AND enabled
             ORDER BY canonical_symbol
            """;

    private InstrumentRegistry() {
    }

    public static List<Instrument> loadEnabled(IngestorConfig config, Exchange exchange) {
        List<Instrument> instruments = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(
                        config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
                PreparedStatement statement = connection.prepareStatement(QUERY)) {
            statement.setString(1, exchange.name());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    instruments.add(new Instrument(
                            exchange, rows.getString("canonical_symbol"), rows.getString("exchange_symbol")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not read the instrument registry from " + config.jdbcUrl(), e);
        }
        if (instruments.isEmpty()) {
            throw new IllegalStateException(
                    "no enabled instruments for " + exchange + "; nothing to subscribe to");
        }
        return List.copyOf(instruments);
    }

    /**
     * Wire symbol to canonical symbol, for translating an inbound frame back to a key.
     *
     * <p>Keyed on the upper-cased wire symbol because that is the only form guaranteed to
     * match — {@link com.marketstream.common.InstrumentKey} upper-cases too, so a
     * case-difference in either the registry or the frame must not silently miss.
     */
    public static Map<String, String> byExchangeSymbol(List<Instrument> instruments) {
        Map<String, String> mapping = new LinkedHashMap<>();
        for (Instrument instrument : instruments) {
            mapping.put(instrument.exchangeSymbol().toUpperCase(), instrument.canonicalSymbol());
        }
        return Collections.unmodifiableMap(mapping);
    }
}
