package com.marketstream.common;

import java.util.Objects;

/**
 * The partition key for every data topic: {@code KRAKEN|BTC/USD}.
 *
 * <p>This is the linchpin decision of the whole design (section 11.4). Keying by
 * {@code (exchange, instrument)} is what guarantees per-book ordering on a single
 * partition — Kafka orders within a partition only — which is a hard prerequisite for
 * sequence-correct order-book reconstruction. It also keeps each book independently
 * parallelisable and generalises to multiple exchanges without a key-scheme change.
 *
 * <p>A silently malformed key is worse than an exception: it hashes to the wrong
 * partition, which breaks ordering for that book and corrupts reconstruction in ways
 * that surface far from the cause. Every factory method here rejects bad input.
 */
public record InstrumentKey(Exchange exchange, String instrument) {

    /** Separates exchange from instrument. Chosen because it cannot occur in a symbol. */
    public static final char SEPARATOR = '|';

    public InstrumentKey {
        Objects.requireNonNull(exchange, "exchange");
        if (instrument == null || instrument.isBlank()) {
            throw new IllegalArgumentException("instrument must not be blank");
        }
        if (instrument.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException(
                    "instrument must not contain '" + SEPARATOR + "': '" + instrument + "'");
        }
        instrument = instrument.trim().toUpperCase();
    }

    public static InstrumentKey of(Exchange exchange, String instrument) {
        return new InstrumentKey(exchange, instrument);
    }

    /**
     * Parses a wire key such as {@code KRAKEN|BTC/USD}.
     *
     * <p>Splits on the <em>first</em> separator only. Symbols contain {@code /}, and
     * splitting on anything but the first {@code |} would mangle them.
     */
    public static InstrumentKey parse(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        int separator = key.indexOf(SEPARATOR);
        if (separator < 0) {
            throw new IllegalArgumentException(
                    "key must be '<EXCHANGE>" + SEPARATOR + "<INSTRUMENT>', got: '" + key + "'");
        }
        return new InstrumentKey(
                Exchange.parse(key.substring(0, separator)),
                key.substring(separator + 1));
    }

    /** The string actually used as the Kafka record key. */
    public String asKafkaKey() {
        return exchange.name() + SEPARATOR + instrument;
    }

    @Override
    public String toString() {
        return asKafkaKey();
    }
}
