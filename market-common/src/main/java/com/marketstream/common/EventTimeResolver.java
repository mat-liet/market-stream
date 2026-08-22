package com.marketstream.common;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Resolves the authoritative event time for a record, guarding against clock skew
 * and missing timestamps (section 13.1).
 *
 * <p>Event time drives every window assignment, so a single absurd exchange timestamp can
 * push a record into a window years away — advancing stream time and prematurely closing
 * every legitimate window in between. This guard bounds that blast radius.
 *
 * <p>The local processor clock is never consulted. Only exchange time or the ingestor's
 * receive time are used, both stamped upstream of windowing, which is what keeps replay
 * deterministic: re-running the same input years later yields the same answer.
 *
 * <p>Pure function, no clock access — deliberately, so it is trivially testable.
 */
public final class EventTimeResolver {

    /** Beyond this far ahead of ingestion time, exchange time is not believable. */
    public static final Duration MAX_FUTURE_SKEW = Duration.ofSeconds(60);

    /** Beyond this far behind ingestion time, exchange time is not believable. */
    public static final Duration MAX_PAST_SKEW = Duration.ofHours(24);

    private EventTimeResolver() {
    }

    /**
     * The resolved time and where it came from.
     *
     * @param eventTime the time to use for windowing
     * @param source    whether that came from the exchange or was a fallback
     */
    public record Resolution(Instant eventTime, EventTimeSource source) {

        public boolean isFallback() {
            return source == EventTimeSource.INGESTION_FALLBACK;
        }
    }

    /**
     * @param exchangeTime  the exchange's timestamp, or {@code null} if absent
     * @param ingestionTime when the ingestor received the frame; never null
     */
    public static Resolution resolve(Instant exchangeTime, Instant ingestionTime) {
        Objects.requireNonNull(ingestionTime, "ingestionTime");

        if (exchangeTime == null) {
            return new Resolution(ingestionTime, EventTimeSource.INGESTION_FALLBACK);
        }
        if (exchangeTime.isAfter(ingestionTime.plus(MAX_FUTURE_SKEW))
                || exchangeTime.isBefore(ingestionTime.minus(MAX_PAST_SKEW))) {
            return new Resolution(ingestionTime, EventTimeSource.INGESTION_FALLBACK);
        }
        return new Resolution(exchangeTime, EventTimeSource.EXCHANGE);
    }
}
