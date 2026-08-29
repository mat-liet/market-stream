package com.marketstream.processor;

import com.marketstream.avro.RawEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Stream time for the raw topic, taken from when the ingestor received the frame.
 *
 * <p>A raw envelope has no exchange event time — it is still inside the payload, and
 * extracting it is normalisation's job (design doc 12.3). So this sub-topology runs on
 * ingestion time, which is fine because it is stateless: nothing here windows, so stream
 * time only affects bookkeeping, and the authoritative event time is applied a topic later
 * where it actually decides window assignment.
 *
 * <p>What matters is that it is still not the local clock. {@code receivedAt} was stamped
 * upstream and is part of the record, so a replay advances stream time exactly as the
 * original run did.
 */
public final class ReceivedAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof RawEnvelope envelope && envelope.getReceivedAt() != null) {
            return envelope.getReceivedAt().toEpochMilli();
        }
        // A negative return makes Streams drop the record, which would be a silent loss.
        // Inheriting the partition's current time keeps it flowing to the normaliser, which
        // is where a nonsense record is meant to be categorised and routed.
        return partitionTime;
    }
}
