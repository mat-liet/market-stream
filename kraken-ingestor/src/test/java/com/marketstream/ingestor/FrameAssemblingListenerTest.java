package com.marketstream.ingestor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

/**
 * The trap this guards against: the JDK WebSocket splits one message across several
 * {@code onText} calls, so parsing each call independently yields truncated JSON. It shows
 * up only for messages large enough to split — that is, during the busiest markets.
 */
class FrameAssemblingListenerTest {

    /** Runs the handoff inline so assertions do not race the listener. */
    private static final Executor DIRECT = Runnable::run;

    private final List<String> frames = new CopyOnWriteArrayList<>();

    private FrameAssemblingListener listener() {
        return new FrameAssemblingListener(
                bytes -> frames.add(new String(bytes, StandardCharsets.UTF_8)), DIRECT, () -> {});
    }

    @Test
    void joinsAMessageSplitAcrossThreeDeliveriesIntoOneFrame() {
        WebSocket webSocket = mock(WebSocket.class);
        FrameAssemblingListener listener = listener();

        String json = "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\"}]}";
        listener.onText(webSocket, json.substring(0, 20), false);
        listener.onText(webSocket, json.substring(20, 45), false);
        assertThat(frames).as("nothing may be emitted before the last fragment").isEmpty();

        CompletionStage<?> stage = listener.onText(webSocket, json.substring(45), true);

        assertThat(frames).containsExactly(json);
        assertThat(stage).as("the last fragment must return a stage so the socket waits").isNotNull();
    }

    @Test
    void keepsSuccessiveFramesSeparate() {
        WebSocket webSocket = mock(WebSocket.class);
        FrameAssemblingListener listener = listener();

        listener.onText(webSocket, "{\"channel\":", false);
        listener.onText(webSocket, "\"heartbeat\"}", true);
        listener.onText(webSocket, "{\"channel\":\"status\"}", true);

        // A buffer that is not cleared after a complete frame prefixes the next one with it.
        assertThat(frames).containsExactly("{\"channel\":\"heartbeat\"}", "{\"channel\":\"status\"}");
    }

    @Test
    void renewsDemandForEveryDelivery() {
        WebSocket webSocket = mock(WebSocket.class);
        FrameAssemblingListener listener = listener();

        // Without a request the socket simply goes quiet, which is indistinguishable from
        // an idle market until the watchdog fires.
        listener.onOpen(webSocket);
        listener.onText(webSocket, "{\"a\":", false);
        listener.onText(webSocket, "1}", true);

        verify(webSocket, atLeastOnce()).request(anyLong());
    }

    @Test
    void signalsDisconnectionOnCloseAndOnError() {
        WebSocket webSocket = mock(WebSocket.class);
        int[] disconnects = {0};
        FrameAssemblingListener listener =
                new FrameAssemblingListener(bytes -> {}, DIRECT, () -> disconnects[0]++);

        listener.onClose(webSocket, WebSocket.NORMAL_CLOSURE, "bye");
        listener.onError(webSocket, new IllegalStateException("reset by peer"));

        // The supervisor reconnects off this signal; missing it means a silent stall.
        assertThat(disconnects[0]).isEqualTo(2);
    }
}
