package com.marketstream.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Canonical decimal handling for prices, quantities and derived values (section 12.1).
 *
 * <p>Market data never uses {@code float} or {@code double}: binary floating point cannot
 * represent decimal prices exactly, and the drift compounds through VWAP, imbalance and
 * checksum computation.
 *
 * <p><strong>Why {@link #canonical} is mandatory rather than convenient:</strong> Avro's
 * {@code decimal} logical type fixes the scale in the schema, and its
 * {@code DecimalConversion} throws when a {@link BigDecimal}'s scale does not match
 * exactly. {@code new BigDecimal("100.5")} has scale 1 and will not serialise against a
 * {@code decimal(38,18)} field. Every value must pass through {@link #canonical} before
 * it reaches a schema object.
 *
 * <p>The rounding mode is fixed once, here, so that replaying identical input yields
 * identical output (correctness invariant 6).
 */
public final class Decimals {

    private Decimals() {
    }

    /** Scale of every {@code decimal(38,18)} field in the Avro schemas. */
    public static final int SCALE = 18;

    /** Precision of every {@code decimal(38,18)} field in the Avro schemas. */
    public static final int PRECISION = 38;

    /** Fixed so derived values are reproducible across replays. */
    public static final RoundingMode ROUNDING = RoundingMode.HALF_UP;

    public static final BigDecimal ZERO = canonical(BigDecimal.ZERO);

    /** Normalises to the canonical scale, rounding only when scaling down. */
    public static BigDecimal canonical(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        return value.setScale(SCALE, ROUNDING);
    }

    /** Parses a wire string (Kraken sends decimals as JSON numbers/strings) to canonical scale. */
    public static BigDecimal parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("decimal must not be blank");
        }
        return canonical(new BigDecimal(value.trim()));
    }

    /**
     * Divides at canonical scale — used for VWAP ({@code quoteVolume / volume}).
     *
     * @return {@link #ZERO} when the divisor is zero, since a window with no volume has no
     *         meaningful VWAP and must not throw mid-aggregation.
     */
    public static BigDecimal divide(BigDecimal dividend, BigDecimal divisor) {
        if (divisor == null || divisor.signum() == 0) {
            return ZERO;
        }
        return dividend.divide(divisor, SCALE, ROUNDING);
    }
}
