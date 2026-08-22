package com.marketstream.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DecimalsTest {

    @Test
    @DisplayName("scales up to the canonical scale Avro requires")
    void scalesUp() {
        assertThat(Decimals.canonical(new BigDecimal("100.5")).scale()).isEqualTo(Decimals.SCALE);
    }

    @Test
    @DisplayName("scales down with the fixed rounding mode")
    void scalesDown() {
        BigDecimal tooPrecise = new BigDecimal("0.1234567890123456789");

        BigDecimal result = Decimals.canonical(tooPrecise);

        assertThat(result.scale()).isEqualTo(Decimals.SCALE);
        assertThat(result).isEqualTo(new BigDecimal("0.123456789012345679"));
    }

    @Test
    @DisplayName("values differing only in scale become equal under equals()")
    void normalisesScaleDifferences() {
        // BigDecimal.equals compares scale, so these differ before canonicalisation.
        assertThat(new BigDecimal("100.5")).isNotEqualTo(new BigDecimal("100.50"));

        assertThat(Decimals.canonical(new BigDecimal("100.5")))
                .isEqualTo(Decimals.canonical(new BigDecimal("100.50")));
    }

    @Test
    @DisplayName("divides at canonical scale for VWAP")
    void dividesAtCanonicalScale() {
        BigDecimal quoteVolume = Decimals.canonical(new BigDecimal("100"));
        BigDecimal volume = Decimals.canonical(new BigDecimal("4"));

        BigDecimal vwap = Decimals.divide(quoteVolume, volume);

        assertThat(vwap.scale()).isEqualTo(Decimals.SCALE);
        assertThat(vwap).isEqualTo(Decimals.canonical(new BigDecimal("25")));
    }

    @Test
    @DisplayName("a non-terminating division does not throw")
    void handlesNonTerminatingDivision() {
        BigDecimal oneThird = Decimals.divide(
                Decimals.canonical(BigDecimal.ONE), Decimals.canonical(new BigDecimal("3")));

        assertThat(oneThird.scale()).isEqualTo(Decimals.SCALE);
        assertThat(oneThird).isEqualTo(new BigDecimal("0.333333333333333333"));
    }

    @Test
    @DisplayName("zero volume yields zero VWAP rather than throwing mid-aggregation")
    void zeroDivisorYieldsZero() {
        assertThat(Decimals.divide(Decimals.canonical(BigDecimal.TEN), Decimals.ZERO))
                .isEqualTo(Decimals.ZERO);
    }

    @Test
    @DisplayName("parses wire strings to canonical scale")
    void parsesWireStrings() {
        assertThat(Decimals.parse(" 64250.10 ")).isEqualTo(Decimals.canonical(new BigDecimal("64250.1")));
    }

    @Test
    @DisplayName("rejects blank and null input")
    void rejectsBadInput() {
        assertThatThrownBy(() -> Decimals.parse("  ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Decimals.canonical(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
