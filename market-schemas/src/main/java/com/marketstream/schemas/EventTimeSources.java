package com.marketstream.schemas;

/**
 * Bridges the plain {@link com.marketstream.common.EventTimeSource} enum to the Avro
 * enum generated from {@code EventHeader.avsc}.
 *
 * <p>The two exist separately on purpose: {@code market-common} is depended on by every
 * service and must not drag Avro in with it. This mapper is the single place they meet,
 * and the exhaustive switch means adding a symbol to either side fails compilation here
 * rather than silently mapping to a wrong value.
 */
public final class EventTimeSources {

    private EventTimeSources() {
    }

    public static com.marketstream.avro.EventTimeSource toAvro(
            com.marketstream.common.EventTimeSource source) {
        return switch (source) {
            case EXCHANGE -> com.marketstream.avro.EventTimeSource.EXCHANGE;
            case INGESTION_FALLBACK -> com.marketstream.avro.EventTimeSource.INGESTION_FALLBACK;
        };
    }

    public static com.marketstream.common.EventTimeSource fromAvro(
            com.marketstream.avro.EventTimeSource source) {
        return switch (source) {
            case EXCHANGE -> com.marketstream.common.EventTimeSource.EXCHANGE;
            case INGESTION_FALLBACK -> com.marketstream.common.EventTimeSource.INGESTION_FALLBACK;
        };
    }
}
