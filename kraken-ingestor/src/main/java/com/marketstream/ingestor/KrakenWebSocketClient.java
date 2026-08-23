package com.marketstream.ingestor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens Kraken WS v2 connections and sends subscriptions on them.
 *
 * <p>Subscription messages are built with Jackson rather than string-formatted, so a symbol
 * containing a character that needs escaping cannot produce a subtly malformed request that
 * Kraken answers with a generic error.
 */
public final class KrakenWebSocketClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KrakenWebSocketClient.class);

    private final IngestorConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger requestId = new AtomicInteger();

    public KrakenWebSocketClient(IngestorConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(config.connectTimeout())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    public WebSocket connect(WebSocket.Listener listener) {
        log.info("connecting to {}", config.websocketUrl());
        return httpClient.newWebSocketBuilder()
                .connectTimeout(config.connectTimeout())
                .buildAsync(URI.create(config.websocketUrl()), listener)
                .join();
    }

    /**
     * Subscribes to trades.
     *
     * <p>{@code snapshot: false} is stated rather than left to the default. With a snapshot,
     * every reconnect replays the last 50 trades, and phase 1 has no deduplication — those
     * repeats would be counted again into candle volume and VWAP. Revisit once phase 2 adds
     * a dedupe store, when the backfill becomes useful instead of corrupting.
     */
    public void subscribeTrades(WebSocket webSocket, List<String> symbols) {
        subscribe(webSocket, FrameClassifier.CHANNEL_TRADE, symbols, false, null);
    }

    /**
     * Subscribes to the order book.
     *
     * <p>Here {@code snapshot: true}, the opposite of trades, because a stream of book
     * deltas without the snapshot they apply to is unusable — and unlike trades, nothing
     * consumes this topic in phase 1, so a repeated snapshot after a reconnect double-counts
     * nothing. The capture exists to answer whether Kraken's book updates carry a sequence
     * number, which decides how divergence is detected in phase 3.
     */
    public void subscribeBook(WebSocket webSocket, List<String> symbols) {
        subscribe(webSocket, FrameClassifier.CHANNEL_BOOK, symbols, true, config.bookDepth());
    }

    private void subscribe(
            WebSocket webSocket, String channel, List<String> symbols, boolean snapshot, Integer depth) {

        ObjectNode params = mapper.createObjectNode();
        params.put("channel", channel);
        ArrayNode symbolArray = params.putArray("symbol");
        symbols.forEach(symbolArray::add);
        params.put("snapshot", snapshot);
        if (depth != null) {
            params.put("depth", depth);
        }

        ObjectNode request = mapper.createObjectNode();
        request.put("method", "subscribe");
        request.set("params", params);
        request.put("req_id", requestId.incrementAndGet());

        String message = request.toString();
        log.info("subscribing: {}", message);
        // Sends are joined one at a time: overlapping sendText calls on the same WebSocket
        // are not permitted and fail the connection rather than queueing.
        webSocket.sendText(message, true).join();
    }

    @Override
    public void close() {
        httpClient.close();
    }
}
