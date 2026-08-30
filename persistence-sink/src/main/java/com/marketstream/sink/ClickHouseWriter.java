package com.marketstream.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes one batch of rows into one ClickHouse table.
 *
 * <p>One writer belongs to one {@link TopicSink}, and therefore to one thread — a shared
 * {@link Connection} is not thread-safe, and a connection pool would buy nothing here because
 * there is never more than one insert in flight per table.
 *
 * <p>The connection is opened lazily and re-opened on failure rather than held open and
 * trusted. A ClickHouse restart leaves a socket that looks alive and fails on first use, and
 * the sink's whole backpressure story depends on that failure being retryable rather than
 * permanent, so every failed batch discards its connection and the next attempt starts from a
 * fresh one.
 *
 * <p>Batches are inserted, never upserted: the table is a {@code ReplacingMergeTree} keyed on
 * natural identity, so a redelivered row overwrites its predecessor on merge (design doc
 * 19.2). There is deliberately no {@code ON DUPLICATE} equivalent to get wrong.
 */
final class ClickHouseWriter<V extends SpecificRecord> implements BatchWriter<V>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseWriter.class);

    private final SinkConfig config;
    private final ClickHouseTable<V> table;

    private Connection connection;

    ClickHouseWriter(SinkConfig config, ClickHouseTable<V> table) {
        this.config = config;
        this.table = table;
    }

    /**
     * Inserts every row in the batch, or throws having inserted none that the caller may rely
     * on. The caller must not commit its Kafka offsets until this returns normally.
     */
    @Override
    public void write(List<V> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try {
            Connection open = connection();
            try (PreparedStatement statement = open.prepareStatement(table.insertSql())) {
                for (V row : rows) {
                    table.bind(statement, row);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        } catch (SQLException e) {
            // The connection may or may not be usable; assuming it is not is cheap and
            // assuming it is costs another full batch of latency to find out.
            discardConnection();
            throw e;
        }
    }

    private Connection connection() throws SQLException {
        if (connection != null && connection.isValid(2)) {
            return connection;
        }
        discardConnection();
        Properties properties = new Properties();
        properties.setProperty("user", config.clickHouseUser());
        properties.setProperty("password", config.clickHousePassword());
        connection = DriverManager.getConnection(config.clickHouseUrl(), properties);
        log.info("connected to ClickHouse at {} for {}", config.clickHouseUrl(), table.tableName());
        return connection;
    }

    private void discardConnection() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            log.debug("closing a failed ClickHouse connection threw, which changes nothing", e);
        }
        connection = null;
    }

    @Override
    public void close() {
        discardConnection();
    }
}
