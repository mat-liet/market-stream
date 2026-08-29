package com.marketstream.processor;

import com.marketstream.avro.InvalidReason;
import com.marketstream.avro.RawEnvelope;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Event ids derived from content rather than drawn at random.
 *
 * <p>Correctness invariant 6 (design doc 23.5) requires that replaying identical raw input
 * yields identical finalised derived output. A {@code UUID.randomUUID()} in the header
 * breaks that on the very first field: the same trades replayed would produce candles that
 * differ, and the deterministic fixture test could only ever assert equality on the subset
 * of fields it chose to ignore. Deriving the id from the event's own identity makes the
 * invariant literal — byte-identical output, with {@code processingTime} the single
 * documented exception because the header defines it as observability-only.
 *
 * <p>It also means a redelivery carries the id it had the first time, which gives the
 * ClickHouse sink a natural idempotency key for free.
 *
 * <p>These are name-based (version 3) UUIDs. They are not secrets and collision resistance
 * across a different input space is not a property we need — only that the same input maps
 * to the same id, every time.
 */
public final class DeterministicIds {

    private DeterministicIds() {
    }

    /**
     * A trade's identity is its dedupe key, which is exactly the definition the dedupe store
     * and any idempotent sink already share.
     */
    public static UUID forTrade(String dedupeKey) {
        return from("trade:" + dedupeKey);
    }

    /**
     * A candle's identity is its coordinates. {@code isFinal} is part of them because the
     * provisional and final records for a window are different events on the same topic;
     * giving them one id would make them indistinguishable to anything deduping by id.
     */
    public static UUID forCandle(String instrumentKey, String window, long windowStart, boolean isFinal) {
        return from("candle:" + instrumentKey + ':' + window + ':' + windowStart + ':' + isFinal);
    }

    /**
     * A rejection's identity is the frame it came from plus what was wrong with it. The
     * connection id and ingest sequence pin the frame to one position in one connection's
     * stream, which is unique and — unlike a wall clock — survives replay unchanged.
     */
    public static UUID forInvalid(RawEnvelope envelope, InvalidReason reason, String detail) {
        return from("invalid:" + envelope.getSourceConnectionId()
                + ':' + envelope.getIngestSequence()
                + ':' + reason.name()
                + ':' + (detail == null ? "" : detail));
    }

    private static UUID from(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }
}
