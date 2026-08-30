package com.marketstream.sink;

import java.sql.SQLException;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;

/**
 * The one thing {@link TopicSink} needs from a database.
 *
 * <p>Narrow on purpose: it is what lets {@code TopicSinkTest} drive the whole poll/batch/
 * commit/backpressure loop against a stub that fails on demand, without a ClickHouse
 * anywhere. The loop's behaviour under a failing write is the part most worth testing and the
 * part hardest to provoke against a real database.
 */
@FunctionalInterface
interface BatchWriter<V extends SpecificRecord> {

    /**
     * Writes every row, or throws. Throwing must mean "try again later": the sink retries
     * indefinitely, which is only safe because {@link ClickHouseTable#inspect} has already
     * removed every row that could fail permanently.
     */
    void write(List<V> rows) throws SQLException;
}
