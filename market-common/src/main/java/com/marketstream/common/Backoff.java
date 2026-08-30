package com.marketstream.common;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential retry delay with jitter.
 *
 * <p>Uses <em>equal jitter</em> — half the ceiling plus a random share of the other half —
 * rather than full jitter. Full jitter can return a near-zero delay on the tenth attempt,
 * which turns a remote outage into a retry storm at exactly the moment the remote least
 * wants one. Equal jitter keeps consecutive attempts in non-overlapping ranges, so the delay
 * genuinely grows, while still spreading retries out so a restart of several instances does
 * not synchronise them.
 *
 * <p>Lives here rather than in one service because both retry loops in the platform want the
 * same curve: the ingestor's Kraken reconnect and the sink's ClickHouse write. The reasoning
 * above is identical for both, and two copies of it would drift.
 *
 * <p>Not thread-safe; one instance belongs to one loop.
 */
public final class Backoff {

    private final long initialMillis;
    private final long maxMillis;
    private final DoubleSupplier jitter;

    private int attempt;

    public Backoff(Duration initial, Duration max) {
        this(initial, max, () -> ThreadLocalRandom.current().nextDouble());
    }

    /** @param jitter supplies a value in {@code [0, 1)}; injected so the test is deterministic */
    public Backoff(Duration initial, Duration max, DoubleSupplier jitter) {
        if (initial.isNegative() || initial.isZero()) {
            throw new IllegalArgumentException("initial backoff must be positive");
        }
        if (max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("max backoff must not be below the initial backoff");
        }
        this.initialMillis = initial.toMillis();
        this.maxMillis = max.toMillis();
        this.jitter = jitter;
    }

    /** The delay before the next attempt, advancing the sequence. */
    public Duration nextDelay() {
        long ceiling = ceilingFor(attempt);
        attempt++;
        long half = ceiling / 2;
        return Duration.ofMillis(half + (long) (jitter.getAsDouble() * (ceiling - half)));
    }

    /** The upper bound for a given attempt, exposed so the test can assert the range. */
    public long ceilingFor(int attempt) {
        if (attempt >= 63) {
            return maxMillis;
        }
        long uncapped = initialMillis << attempt;
        // Shifting past the sign bit yields a negative value long before attempt 63.
        return uncapped <= 0 ? maxMillis : Math.min(uncapped, maxMillis);
    }

    /** Called after an attempt has proved itself, so the next outage starts small again. */
    public void reset() {
        attempt = 0;
    }

    public int attempt() {
        return attempt;
    }
}
