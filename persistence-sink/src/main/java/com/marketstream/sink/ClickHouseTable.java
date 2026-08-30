package com.marketstream.sink;

import com.marketstream.common.Decimals;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;

/**
 * How one Kafka record type becomes one ClickHouse row.
 *
 * <p>All table knowledge lives behind this interface so {@link TopicSink} can stay generic:
 * the loop knows about polling, batching, committing and backpressure, and nothing about
 * columns. Adding {@code alerts} or {@code derived.book.metrics} in a later phase is one more
 * implementation, not another loop.
 *
 * <p>{@link #inspect} runs <em>before</em> the row enters a batch, and it is what makes the
 * sink's unbounded write retry safe. The sink holds read-only Kafka ACLs (design doc 22), so
 * it cannot route a bad record to {@code invalid.events}; if a permanently-failing row could
 * reach the {@code INSERT}, a forever-retrying loop would block the pipeline for good. By
 * checking every value against its column here, any failure the {@code INSERT} does hit is
 * transient by construction, and therefore worth retrying forever.
 */
public interface ClickHouseTable<V extends SpecificRecord> {

    /** What {@link #inspect} decided about one record. */
    sealed interface Verdict {

        /** Bind it and write it. */
        record Store() implements Verdict {
        }

        /**
         * Not an error: this record is deliberately not persisted in this phase, and its
         * offset should be committed as though it had been.
         */
        record Skip(String reason) implements Verdict {
        }

        /**
         * The record cannot be represented in the table. It is logged in full, counted, and
         * dropped — a visible data-quality event rather than a database error.
         */
        record Reject(String reason) implements Verdict {
        }

        Verdict STORE = new Store();
    }

    /** Fully qualified, e.g. {@code market.trades}. */
    String tableName();

    /**
     * Column names in bind order. Quoted where ClickHouse needs it.
     *
     * <p>{@code written_at} is deliberately absent from every implementation: leaving it to
     * the column's {@code DEFAULT now64(3)} is what makes a replayed row outrank the row it
     * replaces on a {@code ReplacingMergeTree} merge. Supplying it from the record would make
     * the two rows tie, and which one survived would be arbitrary.
     */
    List<String> columns();

    Verdict inspect(V value);

    /** Binds one record's values, in {@link #columns} order, starting at parameter 1. */
    void bind(PreparedStatement statement, V value) throws SQLException;

    /** The parameterised {@code INSERT} this table batches into. */
    default String insertSql() {
        return "INSERT INTO " + tableName() + " ("
                + String.join(", ", columns())
                + ") VALUES ("
                + String.join(", ", Collections.nCopies(columns().size(), "?"))
                + ")";
    }

    // ---------------------------------------------------------------- shared checks ----

    /**
     * Whether a value fits {@code Decimal(38, 18)}.
     *
     * <p>The check is on the canonical form, because scale is what determines how many of the
     * 38 digits the fractional part consumes. An uncanonicalised {@code BigDecimal} of scale 2
     * would pass a naive precision test and then overflow once ClickHouse scaled it to 18.
     */
    static boolean fitsDecimal(BigDecimal value) {
        if (value == null) {
            return false;
        }
        // precision() counts the digits of the unscaled value, which at scale 18 is exactly
        // what has to fit inside Decimal(38, 18)'s 38.
        return Decimals.canonical(value).precision() <= Decimals.PRECISION;
    }

    /**
     * Whether an epoch-millis timestamp is representable as {@code DateTime64(3)}, whose range
     * is 1900-01-01 to 2299-12-31. A trade dated outside that is not late data, it is a
     * corrupt clock, and storing it would silently create a partition centuries away.
     */
    static boolean fitsDateTime64(long epochMillis) {
        return epochMillis >= -2_208_988_800_000L && epochMillis <= 10_413_791_999_000L;
    }

    /**
     * Converts to what clickhouse-jdbc binds unambiguously into {@code DateTime64(3)}.
     *
     * <p>A bare {@code Instant} or a {@code java.sql.Timestamp} invites the driver to
     * reinterpret the value against the server's timezone; a {@code LocalDateTime} at UTC is
     * taken literally, which is what the column means.
     */
    static LocalDateTime atUtc(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }
}
