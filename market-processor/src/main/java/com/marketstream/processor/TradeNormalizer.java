package com.marketstream.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketstream.avro.EventHeader;
import com.marketstream.avro.InvalidEvent;
import com.marketstream.avro.InvalidReason;
import com.marketstream.avro.ProcessingStage;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.Side;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import com.marketstream.common.EventTimeResolver;
import com.marketstream.common.Exchange;
import com.marketstream.common.InstrumentKey;
import com.marketstream.common.Topics;
import com.marketstream.schemas.EventTimeSources;
import com.marketstream.schemas.SchemaVersions;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns one raw Kraken frame into canonical {@link TradeEvent}s (design doc 6.2, 12.4).
 *
 * <p>This is the only place in the system that interprets exchange JSON. The ingestor
 * deliberately does not, so that {@code raw.*} stays a byte-faithful replay source; that
 * makes every judgement here revisable by replay, and makes this class the one worth
 * testing hardest.
 *
 * <p>A pure function with no clock, no Kafka and no I/O (design doc 23.1). Event time comes
 * from the frame or from the envelope's receive time, never from the local clock, which is
 * what lets a replay years later produce the same windows.
 *
 * <p>Nothing here throws for bad input. A malformed or unrecognised frame is a routine
 * outcome with its own destination, not an error that should take the topology down
 * (design doc 18, scenario 8).
 */
public final class TradeNormalizer {

    /**
     * Without this, Jackson parses a JSON float into a {@code double} and the exact decimal
     * is gone before we ever see it — {@code DoubleNode.decimalValue()} then returns a
     * faithful rendering of a value that is already wrong. Kraken sends {@code price} and
     * {@code qty} as JSON floats, so this single feature is what stands between the design's
     * decimal guarantee and silent rounding drift through every VWAP downstream.
     */
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final String processorVersion;

    public TradeNormalizer(String processorVersion) {
        this.processorVersion = processorVersion;
    }

    /**
     * @return one result per trade in the frame, or exactly one rejection. Never empty,
     *         because "this frame produced nothing" and "this frame was silently dropped"
     *         must not look the same from the outside.
     */
    public List<NormalizationResult> normalize(String recordKey, RawEnvelope envelope) {
        JsonNode root;
        try {
            root = MAPPER.readTree(payloadOf(envelope));
        } catch (Exception e) {
            // Not even JSON. There is nothing to categorise, so it goes to dead-letter with
            // its bytes intact rather than to the analytical DLQ.
            return List.of(new NormalizationResult.Unparseable(recordKey, envelope));
        }
        if (root == null || !root.isObject()) {
            return List.of(new NormalizationResult.Unparseable(recordKey, envelope));
        }

        String channel = text(root, "channel");
        if (!"trade".equals(channel)) {
            // We only subscribed this sub-topology to raw.kraken.trade, so a non-trade frame
            // here means either a routing mistake or a new Kraken message type. Both are
            // worth seeing in invalid.events rather than dropping.
            return List.of(reject(recordKey, envelope, InvalidReason.UNKNOWN_TYPE, null,
                    "channel=" + channel));
        }

        JsonNode data = root.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            return List.of(reject(recordKey, envelope, InvalidReason.SCHEMA_INVALID, null,
                    "trade frame has no data array"));
        }

        List<NormalizationResult> results = new ArrayList<>(data.size());
        for (int ordinal = 0; ordinal < data.size(); ordinal++) {
            results.add(normalizeOne(recordKey, envelope, data.get(ordinal), ordinal));
        }
        return results;
    }

    private NormalizationResult normalizeOne(
            String recordKey, RawEnvelope envelope, JsonNode node, int ordinal) {

        String symbol = text(node, "symbol");
        if (symbol == null || symbol.isBlank()) {
            return reject(recordKey, envelope, InvalidReason.SCHEMA_INVALID, null,
                    "trade at index " + ordinal + " has no symbol");
        }

        InstrumentKey key;
        try {
            key = InstrumentKey.of(Exchange.KRAKEN, symbol);
        } catch (RuntimeException e) {
            return reject(recordKey, envelope, InvalidReason.SCHEMA_INVALID, null,
                    "unusable symbol '" + symbol + "': " + e.getMessage());
        }

        Side side = sideOf(text(node, "side"));
        if (side == null) {
            return reject(recordKey, envelope, InvalidReason.SCHEMA_INVALID, key.instrument(),
                    "unknown side '" + text(node, "side") + "'");
        }

        BigDecimal price = decimal(node, "price");
        BigDecimal quantity = decimal(node, "qty");
        if (price == null || quantity == null) {
            return reject(recordKey, envelope, InvalidReason.SCHEMA_INVALID, key.instrument(),
                    "price or qty missing or not numeric");
        }

        // A trade with no size is not a trade, and folding one in would add a row to
        // tradeCount while contributing nothing to volume — quietly skewing VWAP's weighting.
        if (quantity.signum() <= 0 || price.signum() <= 0) {
            return reject(recordKey, envelope, InvalidReason.IMPOSSIBLE_STATE, key.instrument(),
                    "non-positive price or quantity: " + price + " @ " + quantity);
        }

        EventTimeResolver.Resolution time =
                EventTimeResolver.resolve(exchangeTime(node), envelope.getReceivedAt());

        // Canonicalised before the id is built, not after. Jackson normalises trailing zeros
        // when it parses, so the same trade written as 60000 and as 60000.00 arrives as two
        // different BigDecimals; composing an id from those renderings would hand the same
        // trade two identities, which is precisely what the composite exists to prevent.
        BigDecimal canonicalPrice = Decimals.canonical(price);
        BigDecimal canonicalQuantity = Decimals.canonical(quantity);

        String tradeId = tradeIdOf(
                node, key, time.eventTime(), canonicalPrice, canonicalQuantity, side, ordinal);
        String dedupeKey = key.instrument() + '|' + tradeId;

        EventHeader header = EventHeader.newBuilder()
                .setEventId(DeterministicIds.forTrade(key.asKafkaKey() + '|' + tradeId))
                .setExchange(com.marketstream.avro.Exchange.KRAKEN)
                .setInstrument(key.instrument())
                .setEventTime(time.eventTime())
                .setIngestionTime(envelope.getReceivedAt())
                // Left null deliberately. processingTime is wall-clock and would be the one
                // field making normalized.trades differ between two replays of identical
                // input; the schema scopes it to derived events, and the end-to-end latency
                // metric is recorded directly rather than read back off this field.
                .setProcessingTime(null)
                .setEventTimeSource(EventTimeSources.toAvro(time.source()))
                .setSchemaVersion(SchemaVersions.CURRENT)
                .setTraceId(envelope.getTraceId())
                .setSourceEventId(envelope.getEventId())
                .build();

        TradeEvent trade = TradeEvent.newBuilder()
                .setHeader(header)
                .setTradeId(tradeId)
                .setPrice(canonicalPrice)
                .setQuantity(canonicalQuantity)
                .setSide(side)
                .setDedupeKey(dedupeKey)
                .build();

        return new NormalizationResult.Normalized(key.asKafkaKey(), trade);
    }

    /**
     * Kraken v2 carries {@code trade_id}, so the composite is a fallback we expect never to
     * take. It still has to be right: the ordinal is what separates two trades that share a
     * millisecond, price, size and side, and it is needed because {@code eventTime} is
     * milliseconds while Kraken timestamps are microseconds — the sub-millisecond digits
     * that would otherwise tell them apart are gone by this point.
     *
     * <p>{@code price} and {@code quantity} must already be canonical, so that how the
     * exchange happened to write a number does not change the identity of the trade.
     */
    private static String tradeIdOf(
            JsonNode node,
            InstrumentKey key,
            Instant eventTime,
            BigDecimal price,
            BigDecimal quantity,
            Side side,
            int ordinal) {

        JsonNode id = node.get("trade_id");
        if (id != null && !id.isNull()) {
            // asText() and never asLong(): Kraken ids are unsigned 64-bit elsewhere in the
            // protocol, and the id is an opaque identifier here regardless.
            String value = id.asText();
            if (!value.isBlank()) {
                return value;
            }
        }
        return "c:" + key.instrument()
                + ':' + eventTime.toEpochMilli()
                + ':' + price.toPlainString()
                + ':' + quantity.toPlainString()
                + ':' + side.name()
                + ':' + ordinal;
    }

    private NormalizationResult.Rejected reject(
            String recordKey,
            RawEnvelope envelope,
            InvalidReason reason,
            String instrument,
            String detail) {

        InvalidEvent invalid = InvalidEvent.newBuilder()
                .setEventId(DeterministicIds.forInvalid(envelope, reason, detail))
                // The envelope's receive time, not the wall clock: it is both deterministic
                // under replay and the more useful answer to "when did this bad data arrive".
                .setOccurredAt(envelope.getReceivedAt())
                .setStage(ProcessingStage.NORMALIZE)
                .setReason(reason)
                .setErrorClass(detail)
                .setOriginalTopic(Topics.RAW_KRAKEN_TRADE)
                .setOriginalKey(recordKey)
                .setOriginalPayload(envelope.getPayload().duplicate())
                .setInstrument(instrument)
                .setTraceId(envelope.getTraceId())
                .setProcessorVersion(processorVersion)
                .setSchemaVersion(SchemaVersions.CURRENT)
                .build();

        // invalid.events is keyed by exchange|instrument where we have one; an unresolvable
        // instrument keeps the incoming key so the record still lands somewhere traceable.
        String key = instrument == null
                ? recordKey
                : InstrumentKey.of(Exchange.KRAKEN, instrument).asKafkaKey();
        return new NormalizationResult.Rejected(key, invalid);
    }

    private static Instant exchangeTime(JsonNode node) {
        String value = text(node, "timestamp");
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            // Kraken sends microsecond precision but eventTime is timestamp-millis, so the
            // truncation is going to happen either way. Doing it here rather than leaving it
            // to the serialiser keeps the in-memory record equal to its own round trip,
            // which is what lets the fixture test compare records instead of bytes.
            return Instant.parse(value).truncatedTo(ChronoUnit.MILLIS);
        } catch (DateTimeParseException e) {
            // Indistinguishable from an absent timestamp as far as windowing is concerned:
            // EventTimeResolver falls back to ingestion time and flags the record.
            return null;
        }
    }

    private static Side sideOf(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case "buy", "b" -> Side.BUY;
            case "sell", "s" -> Side.SELL;
            default -> null;
        };
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.decimalValue();
        }
        if (value.isTextual()) {
            // Kraken v2 sends numbers, but a string is exactly representable and harmless
            // to accept — and this is the shape a future exchange adapter is likelier to use.
            try {
                return new BigDecimal(value.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || !value.isTextual() ? null : value.asText();
    }

    private static String payloadOf(RawEnvelope envelope) {
        // duplicate() so reading the payload does not drain the buffer for anything else
        // holding the same envelope — notably the dead-letter path, which forwards it on.
        ByteBuffer buffer = envelope.getPayload().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
