package com.marketstream.processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Which build is running, read from the Postgres processor-version registry (design doc 15.3).
 *
 * <p>Every {@code Candle} carries this string so a wrong number is traceable to the code
 * that produced it. That only works if the value is a fact about a registered deploy rather
 * than a string the process made up about itself, which is why it is read from the registry
 * and why an unregistered deploy fails to start instead of stamping something unverifiable
 * onto derived data.
 *
 * <p>Read once at startup. Plain JDBC, for the same reason as
 * {@code kraken-ingestor}'s instrument registry: one query does not justify a pool.
 */
public final class ProcessorVersionRegistry {

    /** The live row is the one that has not been retired. */
    private static final String QUERY = """
            SELECT processor_version
              FROM processor_version
             WHERE retired_at IS NULL
            """;

    private ProcessorVersionRegistry() {
    }

    public static String loadLiveVersion(ProcessorConfig config) {
        try (Connection connection = DriverManager.getConnection(
                        config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
                PreparedStatement statement = connection.prepareStatement(QUERY);
                ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new IllegalStateException(
                        "no live row in processor_version; register this build before starting it");
            }
            String version = rows.getString("processor_version");
            if (rows.next()) {
                // The partial unique index only stops the *same* version being live twice.
                // Two different un-retired versions are still possible — a deploy that
                // forgot to retire its predecessor — and picking one arbitrarily would
                // stamp derived data with a version that may not be this code.
                throw new IllegalStateException(
                        "more than one live row in processor_version; retire the old one before deploying");
            }
            return version;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "could not read the processor version registry from " + config.jdbcUrl(), e);
        }
    }
}
