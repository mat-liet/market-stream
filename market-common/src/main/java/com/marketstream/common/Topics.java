package com.marketstream.common;

import java.util.Set;

/**
 * Every Kafka topic in the platform (section 11.2).
 *
 * <p>These constants must stay in lockstep with {@code infra/kafka-init/create-topics.sh},
 * which is what actually creates them. Broker auto-create is disabled, so a typo here does
 * not quietly create a topic with wrong settings — it fails, which is the intent.
 *
 * <p>Topics are separated by category and lifecycle, never by market: markets are
 * distinguished by the partition key ({@link InstrumentKey}), not by topic name.
 */
public final class Topics {

    private Topics() {
    }

    // Raw: immutable replay source of truth. Never mutated, longest affordable retention.
    public static final String RAW_KRAKEN_TRADE = "raw.kraken.trade";
    public static final String RAW_KRAKEN_BOOK = "raw.kraken.book";

    // Normalized: cheap canonical intermediate, always rederivable from raw.
    public static final String NORMALIZED_TRADES = "normalized.trades";
    public static final String NORMALIZED_BOOK_EVENTS = "normalized.book.events";

    // Derived: the product.
    public static final String DERIVED_CANDLES = "derived.candles";
    public static final String DERIVED_BOOK_METRICS = "derived.book.metrics";
    public static final String ALERTS = "alerts";

    // State: a table of latest-per-key, compacted rather than time-retained.
    public static final String STATE_BOOK_CURRENT = "state.book.current";

    // Ops: structured data-quality failures vs frames we could not even categorise.
    public static final String INVALID_EVENTS = "invalid.events";
    public static final String DEAD_LETTER = "dead-letter";

    // Control: processor -> ingestor resubscribe requests (phase 3).
    public static final String CONTROL_BOOK_RESYNC = "control.book.resync";

    /** All topics, for start-up validation against the broker. */
    public static final Set<String> ALL = Set.of(
            RAW_KRAKEN_TRADE,
            RAW_KRAKEN_BOOK,
            NORMALIZED_TRADES,
            NORMALIZED_BOOK_EVENTS,
            DERIVED_CANDLES,
            DERIVED_BOOK_METRICS,
            ALERTS,
            STATE_BOOK_CURRENT,
            INVALID_EVENTS,
            DEAD_LETTER,
            CONTROL_BOOK_RESYNC);
}
