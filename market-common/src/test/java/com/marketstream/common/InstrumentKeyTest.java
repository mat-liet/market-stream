package com.marketstream.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InstrumentKeyTest {

    @Test
    @DisplayName("renders the wire format the partitioner hashes")
    void rendersWireFormat() {
        assertThat(InstrumentKey.of(Exchange.KRAKEN, "BTC/USD").asKafkaKey())
                .isEqualTo("KRAKEN|BTC/USD");
    }

    @Test
    @DisplayName("round-trips through parse")
    void roundTrips() {
        InstrumentKey key = InstrumentKey.of(Exchange.KRAKEN, "ETH/USD");
        assertThat(InstrumentKey.parse(key.asKafkaKey())).isEqualTo(key);
    }

    @Test
    @DisplayName("splits on the first separator so the '/' in a symbol survives")
    void preservesSlashInSymbol() {
        InstrumentKey key = InstrumentKey.parse("KRAKEN|BTC/USD");

        assertThat(key.exchange()).isEqualTo(Exchange.KRAKEN);
        assertThat(key.instrument()).isEqualTo("BTC/USD");
    }

    @Test
    @DisplayName("normalises case so the same market never lands on two partitions")
    void normalisesCase() {
        assertThat(InstrumentKey.parse("kraken|btc/usd").asKafkaKey()).isEqualTo("KRAKEN|BTC/USD");
    }

    @Test
    @DisplayName("rejects a key with no separator rather than guessing")
    void rejectsMissingSeparator() {
        assertThatThrownBy(() -> InstrumentKey.parse("BTC/USD"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("<EXCHANGE>");
    }

    @Test
    @DisplayName("rejects an unknown exchange rather than inventing one")
    void rejectsUnknownExchange() {
        assertThatThrownBy(() -> InstrumentKey.parse("COINBASE|BTC/USD"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown exchange");
    }

    @Test
    @DisplayName("rejects a separator inside the instrument")
    void rejectsSeparatorInInstrument() {
        assertThatThrownBy(() -> InstrumentKey.of(Exchange.KRAKEN, "BTC|USD"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("rejects blank input")
    void rejectsBlank() {
        assertThatThrownBy(() -> InstrumentKey.parse("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstrumentKey.of(Exchange.KRAKEN, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
