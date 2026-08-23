package com.marketstream.ingestor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private static final Duration INITIAL = Duration.ofMillis(500);
    private static final Duration MAX = Duration.ofSeconds(30);

    private static Backoff withJitter(double fixed) {
        return new Backoff(INITIAL, MAX, () -> fixed);
    }

    @Test
    void staysWithinTheCeilingForEveryJitterValue() {
        for (double jitter : new double[] {0.0, 0.25, 0.5, 0.999}) {
            Backoff backoff = withJitter(jitter);
            for (int attempt = 0; attempt < 20; attempt++) {
                long ceiling = backoff.ceilingFor(attempt);
                long delay = backoff.nextDelay().toMillis();
                assertThat(delay)
                        .as("attempt %d with jitter %s", attempt, jitter)
                        .isBetween(ceiling / 2, ceiling);
            }
        }
    }

    @Test
    void neverExceedsTheConfiguredMaximum() {
        Backoff backoff = withJitter(0.999);
        for (int attempt = 0; attempt < 200; attempt++) {
            assertThat(backoff.nextDelay()).isLessThanOrEqualTo(MAX);
        }
    }

    @Test
    void growsUntilItReachesTheCap() {
        Backoff backoff = withJitter(0.0);
        List<Long> delays = new ArrayList<>();
        for (int attempt = 0; attempt < 10; attempt++) {
            delays.add(backoff.nextDelay().toMillis());
        }

        // Equal jitter is chosen precisely so this holds: with full jitter a late attempt
        // can return a near-zero delay and turn an exchange outage into a reconnect storm.
        for (int i = 1; i < delays.size(); i++) {
            assertThat(delays.get(i)).isGreaterThanOrEqualTo(delays.get(i - 1));
        }
        assertThat(delays.get(delays.size() - 1)).isEqualTo(MAX.toMillis() / 2);
    }

    @Test
    void separateAttemptsDoNotOverlapBeforeTheCap() {
        // The lower bound of one attempt is the upper bound of the previous one, so the
        // sequence genuinely backs off rather than merely trending upwards.
        Backoff floors = withJitter(0.0);
        Backoff ceilings = withJitter(0.999);

        long previousCeiling = ceilings.nextDelay().toMillis();
        floors.nextDelay();
        for (int attempt = 1; attempt < 5; attempt++) {
            long floor = floors.nextDelay().toMillis();
            assertThat(floor).isGreaterThanOrEqualTo(previousCeiling);
            previousCeiling = ceilings.nextDelay().toMillis();
        }
    }

    @Test
    void resetReturnsToTheInitialDelay() {
        Backoff backoff = withJitter(0.0);
        for (int attempt = 0; attempt < 8; attempt++) {
            backoff.nextDelay();
        }

        backoff.reset();

        assertThat(backoff.attempt()).isZero();
        assertThat(backoff.nextDelay().toMillis()).isEqualTo(INITIAL.toMillis() / 2);
    }

    @Test
    void rejectsNonsenseBounds() {
        assertThatThrownBy(() -> new Backoff(Duration.ZERO, MAX))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(MAX, INITIAL))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
