package com.marketstream.ingestor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Decides what an inbound Kraken frame is, and nothing more.
 *
 * <p>This is the only place the ingestor looks inside a frame, and it looks at exactly two
 * things: which channel it belongs to (so it reaches the right raw topic) and which symbol
 * it concerns (so it gets the right partition key). Trade semantics — price, quantity,
 * side, timestamps — are deliberately not touched here. {@code raw.*} is the replay source
 * of truth, and every interpretation added at ingest is an interpretation that replay can
 * no longer revise (design doc trade-off 8).
 *
 * <p>Classification never throws. A frame the ingestor cannot understand is still a fact
 * about what the exchange sent, so it is categorised and counted rather than discarded on
 * the floor.
 */
public final class FrameClassifier {

    /**
     * What a frame is.
     *
     * <p>{@link #MALFORMED} is kept distinct from {@link #UNKNOWN} on purpose: unparseable
     * bytes usually mean a framing bug on our side (see
     * {@link FrameAssemblingListener} — a truncated frame looks exactly like this), while
     * a well-formed frame on an unrecognised channel means Kraken shipped something new.
     * Those need different responses, so they must not share a counter.
     */
    public enum FrameType {
        TRADE_DATA,
        BOOK_DATA,
        HEARTBEAT,
        STATUS,
        SUBSCRIBE_ACK,
        UNKNOWN,
        MALFORMED;

        /** Only market data reaches a raw topic. Everything else is observed, not stored. */
        public boolean isPublishable() {
            return this == TRADE_DATA || this == BOOK_DATA;
        }
    }

    /**
     * @param symbol       the exchange's wire symbol, or null when the frame has none.
     *                     Kraken v2 scopes each message to exactly one symbol, which is
     *                     what makes a single key per envelope correct.
     * @param connectionId only present on {@link FrameType#STATUS}. A string, never a
     *                     number — Kraken's connection ids are unsigned 64-bit and the
     *                     documented example {@code 13834774380200032777} is larger than
     *                     {@code Long.MAX_VALUE}, so parsing one as a long would silently
     *                     produce a wrong id rather than fail.
     */
    public record Frame(FrameType type, String channel, String symbol, String connectionId) {

        static Frame of(FrameType type) {
            return new Frame(type, null, null, null);
        }
    }

    public static final String CHANNEL_TRADE = "trade";
    public static final String CHANNEL_BOOK = "book";
    private static final String CHANNEL_HEARTBEAT = "heartbeat";
    private static final String CHANNEL_STATUS = "status";

    private final ObjectMapper mapper = new ObjectMapper();

    public Frame classify(byte[] frame) {
        JsonNode root;
        try {
            root = mapper.readTree(frame);
        } catch (Exception e) {
            return Frame.of(FrameType.MALFORMED);
        }
        if (root == null || !root.isObject()) {
            return Frame.of(FrameType.MALFORMED);
        }

        // Responses to our own subscribe/unsubscribe requests echo the method back.
        // Checked before the channel, because an ack also carries result.channel.
        if (root.hasNonNull("method")) {
            return new Frame(FrameType.SUBSCRIBE_ACK, root.get("method").asText(), null, null);
        }

        JsonNode channelNode = root.get("channel");
        if (channelNode == null || !channelNode.isTextual()) {
            return Frame.of(FrameType.UNKNOWN);
        }
        String channel = channelNode.asText();

        return switch (channel) {
            case CHANNEL_TRADE -> new Frame(FrameType.TRADE_DATA, channel, firstSymbol(root), null);
            case CHANNEL_BOOK -> new Frame(FrameType.BOOK_DATA, channel, firstSymbol(root), null);
            case CHANNEL_HEARTBEAT -> new Frame(FrameType.HEARTBEAT, channel, null, null);
            case CHANNEL_STATUS -> new Frame(FrameType.STATUS, channel, null, connectionId(root));
            default -> new Frame(FrameType.UNKNOWN, channel, firstSymbol(root), null);
        };
    }

    /**
     * The symbol from the first element of {@code data}.
     *
     * <p>Reading only the first element is correct rather than lazy: Kraken v2 scopes a
     * message to one symbol, so every element carries the same one. If that ever stops
     * being true, one envelope would need more than one key and the fix is upstream of
     * here, not a loop.
     */
    private static String firstSymbol(JsonNode root) {
        JsonNode data = root.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            return null;
        }
        JsonNode symbol = data.get(0).get("symbol");
        return symbol != null && symbol.isTextual() ? symbol.asText() : null;
    }

    /** Reads {@code data[0].connection_id} as text. See {@link Frame#connectionId()}. */
    private static String connectionId(JsonNode root) {
        JsonNode data = root.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            return null;
        }
        JsonNode id = data.get(0).get("connection_id");
        return id == null || id.isNull() ? null : id.asText();
    }
}
