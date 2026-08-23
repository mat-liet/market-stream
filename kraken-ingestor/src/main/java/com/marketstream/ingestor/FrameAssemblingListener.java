package com.marketstream.ingestor;

import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reassembles whole JSON frames from the JDK WebSocket's partial deliveries.
 *
 * <p>Two things about {@link WebSocket.Listener} make a naive implementation wrong in ways
 * that only show up under load, which is the worst time to discover them:
 *
 * <ol>
 *   <li><strong>{@code onText} does not receive whole messages.</strong> It receives a
 *       {@link CharSequence} plus a {@code last} flag, and one JSON frame can arrive across
 *       several invocations. Parsing each invocation independently produces truncated-JSON
 *       errors that scale with message size and throughput. Text is therefore accumulated
 *       until {@code last} is true.
 *   <li><strong>Delivery is demand-driven.</strong> Nothing arrives unless
 *       {@link WebSocket#request(long)} has been called, and the connection simply goes
 *       quiet if it is forgotten — indistinguishable from an idle market.
 * </ol>
 *
 * <p>The returned {@link CompletionStage} is the backpressure mechanism. The WebSocket does
 * not invoke the listener again until it completes, and demand is only renewed after the
 * frame has been handed off. So when the downstream queue is full, this stops reading, the
 * TCP receive window closes, and Kraken slows down — rather than the ingestor buffering
 * without bound or dropping frames (design doc 18.1). The handoff runs on a separate
 * executor so the blocking happens there and never on an HTTP client I/O thread.
 */
final class FrameAssemblingListener implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(FrameAssemblingListener.class);

    private final StringBuilder partial = new StringBuilder();
    private final Consumer<byte[]> sink;
    private final Executor handoff;
    private final Runnable onDisconnect;

    /**
     * @param sink         receives each complete frame; may block
     * @param handoff      where {@code sink} runs. Must not be an HTTP client I/O thread.
     * @param onDisconnect run exactly once when the connection ends, however it ends
     */
    FrameAssemblingListener(Consumer<byte[]> sink, Executor handoff, Runnable onDisconnect) {
        this.sink = sink;
        this.handoff = handoff;
        this.onDisconnect = onDisconnect;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (!last) {
            // More of this frame is coming. Ask for it and hold the buffer.
            webSocket.request(1);
            return null;
        }
        byte[] frame = partial.toString().getBytes(StandardCharsets.UTF_8);
        partial.setLength(0);
        return CompletableFuture.runAsync(() -> sink.accept(frame), handoff)
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        log.error("dropping frame: handoff failed", error);
                    }
                    webSocket.request(1);
                });
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        // Kraken v2 is text-only. Renew demand so an unexpected binary frame stalls the
        // connection instead of being mistaken for a dead one.
        log.warn("ignoring unexpected binary frame of {} bytes", data.remaining());
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.info("websocket closed by peer: {} {}", statusCode, reason);
        onDisconnect.run();
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.warn("websocket failed", error);
        onDisconnect.run();
    }
}
