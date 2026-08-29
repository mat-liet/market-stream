package com.marketstream.processor;

import com.marketstream.avro.InvalidEvent;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.TradeEvent;

/**
 * What normalising one raw frame produced. Exactly three outcomes, each with its own topic.
 *
 * <p>Modelled as a sealed type rather than as exceptions because a bad frame is an expected,
 * routine outcome here, not an error: the topology must survive it and keep going (design
 * doc 18, scenario 8). Making the three cases a closed set means the routing switch cannot
 * quietly forget one.
 */
public sealed interface NormalizationResult {

    /**
     * The Kafka key this result must be produced under.
     *
     * <p>Carried on the result rather than recomputed at the sink because the three
     * destinations key differently: data topics by {@code exchange|instrument} so a book
     * stays on one partition, {@code dead-letter} by whatever key the frame arrived with.
     * Null only where no instrument could be resolved, and only ever on an ops topic.
     */
    String key();

    /** A trade, bound for {@code normalized.trades}. */
    record Normalized(String key, TradeEvent trade) implements NormalizationResult {
    }

    /**
     * A frame we understood well enough to categorise but not to use, bound for
     * {@code invalid.events} — the analytical DLQ (design doc 11.7).
     */
    record Rejected(String key, InvalidEvent invalid) implements NormalizationResult {
    }

    /**
     * A frame we could not categorise at all, bound for {@code dead-letter}.
     *
     * <p>It carries the original envelope rather than loose bytes on purpose. The ingestor
     * already publishes {@code RawEnvelope} to {@code dead-letter}, so the registry's
     * {@code dead-letter-value} subject is bound to that schema under BACKWARD
     * compatibility; producing anything else there would be rejected outright. Forwarding
     * the envelope verbatim also keeps the forensic trail intact — {@code sourceConnectionId}
     * and {@code ingestSequence} say exactly which connection delivered the bad frame and
     * where in its stream it sat.
     */
    record Unparseable(String key, RawEnvelope envelope) implements NormalizationResult {
    }
}
