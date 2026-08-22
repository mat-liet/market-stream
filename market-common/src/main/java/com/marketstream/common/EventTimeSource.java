package com.marketstream.common;

/**
 * Where an event's authoritative time came from (section 13.1).
 *
 * <p>Deliberately duplicated: an identically-named enum is generated from
 * {@code EventHeader.avsc} in {@code market-schemas}. This module stays free of Avro so
 * that every service can depend on it without inheriting Kafka or Avro, so the two are
 * bridged by a small mapper in {@code market-schemas} rather than shared directly.
 */
public enum EventTimeSource {

    /** The exchange's own timestamp — the authoritative business time. */
    EXCHANGE,

    /** Exchange time was missing or implausible; the ingestor's receive time was used. */
    INGESTION_FALLBACK
}
