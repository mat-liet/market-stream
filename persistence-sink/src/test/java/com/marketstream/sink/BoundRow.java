package com.marketstream.sink;

import static org.mockito.Mockito.mock;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;

/**
 * Captures what a table binds, so a test can assert on parameter positions.
 *
 * <p>Rather than verifying individual {@code setString}/{@code setBigDecimal} calls one at a
 * time, this records every {@code set*(index, value)} into a map. That makes the important
 * assertion expressible directly: the set of bound positions must be exactly
 * {@code 1..columns().size()}, with no gap and no overrun. A column list and a {@code bind}
 * method are one fact written twice, and the failure mode when they drift — two same-typed
 * columns transposed — is silent and permanent once the rows are stored.
 */
final class BoundRow {

    private BoundRow() {
    }

    static <V extends SpecificRecord> Map<Integer, Object> bind(ClickHouseTable<V> table, V value) {
        Map<Integer, Object> bound = new LinkedHashMap<>();
        PreparedStatement statement = mock(PreparedStatement.class, invocation -> {
            if (invocation.getMethod().getName().startsWith("set")
                    && invocation.getArguments().length == 2
                    && invocation.getArgument(0) instanceof Integer index) {
                bound.put(index, invocation.getArgument(1));
            }
            return null;
        });
        try {
            table.bind(statement, value);
        } catch (Exception e) {
            throw new AssertionError("binding threw", e);
        }
        return bound;
    }
}
