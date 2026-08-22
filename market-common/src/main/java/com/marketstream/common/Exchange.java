package com.marketstream.common;

/**
 * The exchanges this platform can ingest.
 *
 * <p>Only Kraken exists in v1. This is an enum rather than a bare string because it
 * namespaces every partition key ({@link InstrumentKey}) and every raw topic name, so
 * adding a second exchange stays additive rather than becoming a rename.
 */
public enum Exchange {
    KRAKEN;

    /** Parses an exchange name, rejecting anything unknown rather than guessing. */
    public static Exchange parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("exchange must not be blank");
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown exchange: '" + value + "'", e);
        }
    }
}
