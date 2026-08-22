package com.marketstream.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.common.EventTimeResolver.Resolution;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EventTimeResolverTest {

    private static final Instant INGESTED = Instant.parse("2026-08-22T12:00:00Z");

    @Test
    @DisplayName("trusts a plausible exchange timestamp")
    void trustsExchangeTime() {
        Instant exchangeTime = INGESTED.minusMillis(250);

        Resolution resolution = EventTimeResolver.resolve(exchangeTime, INGESTED);

        assertThat(resolution.eventTime()).isEqualTo(exchangeTime);
        assertThat(resolution.source()).isEqualTo(EventTimeSource.EXCHANGE);
    }

    @Test
    @DisplayName("falls back when exchange time is missing")
    void fallsBackOnMissing() {
        Resolution resolution = EventTimeResolver.resolve(null, INGESTED);

        assertThat(resolution.eventTime()).isEqualTo(INGESTED);
        assertThat(resolution.isFallback()).isTrue();
    }

    @Test
    @DisplayName("accepts exactly the future skew limit, rejects just past it")
    void futureSkewBoundary() {
        Instant atLimit = INGESTED.plus(EventTimeResolver.MAX_FUTURE_SKEW);
        Instant beyondLimit = atLimit.plusMillis(1);

        assertThat(EventTimeResolver.resolve(atLimit, INGESTED).source())
                .isEqualTo(EventTimeSource.EXCHANGE);
        assertThat(EventTimeResolver.resolve(beyondLimit, INGESTED).source())
                .isEqualTo(EventTimeSource.INGESTION_FALLBACK);
    }

    @Test
    @DisplayName("accepts exactly the past skew limit, rejects just before it")
    void pastSkewBoundary() {
        Instant atLimit = INGESTED.minus(EventTimeResolver.MAX_PAST_SKEW);
        Instant beyondLimit = atLimit.minusMillis(1);

        assertThat(EventTimeResolver.resolve(atLimit, INGESTED).source())
                .isEqualTo(EventTimeSource.EXCHANGE);
        assertThat(EventTimeResolver.resolve(beyondLimit, INGESTED).source())
                .isEqualTo(EventTimeSource.INGESTION_FALLBACK);
    }

    @Test
    @DisplayName("a wildly future timestamp cannot drag stream time forward")
    void rejectsAbsurdFutureTime() {
        Instant nextYear = INGESTED.plus(Duration.ofDays(365));

        Resolution resolution = EventTimeResolver.resolve(nextYear, INGESTED);

        assertThat(resolution.eventTime()).isEqualTo(INGESTED);
        assertThat(resolution.isFallback()).isTrue();
    }
}
