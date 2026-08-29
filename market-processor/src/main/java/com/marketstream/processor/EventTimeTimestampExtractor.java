package com.marketstream.processor;

import com.marketstream.avro.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Stream time for {@code normalized.trades}: the exchange's event time (design doc 13.1).
 *
 * <p>This is the extractor that decides every window assignment. A trade at 12:00:03.4
 * belongs to the [12:00:00, 12:01:00) window no matter when it is processed, which is the
 * whole basis of reproducible candles.
 *
 * <p>{@code eventTime} has already passed through {@code EventTimeResolver} at
 * normalisation, so an absurd exchange timestamp has been replaced by ingestion time and
 * flagged. Nothing unbounded reaches stream time from here, which matters because a single
 * timestamp years in the future would close every legitimate window between now and then.
 */
public final class EventTimeTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof TradeEvent trade && trade.getHeader().getEventTime() != null) {
            return trade.getHeader().getEventTime().toEpochMilli();
        }
        // Every TradeEvent has a required eventTime, so this is unreachable short of a
        // schema change. Falling back to partition time rather than returning a negative
        // keeps such a record out of the wrong window without dropping it unrecorded.
        return partitionTime;
    }
}
