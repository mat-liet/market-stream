package com.marketstream.ingestor;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the ingestor reports about itself (design doc 21.1).
 *
 * <p>The counters are chosen so that the two failure modes that matter are visible without
 * reading logs: frames arriving but not being published (queue depth climbing, publish
 * failures rising) and a connection that is nominally up but receiving nothing
 * ({@code frames_received} flat while {@code connection_up} is 1).
 *
 * <p>Frames are counted by classified type, not just in total, because "we are receiving
 * plenty of frames" is not reassuring if all of them are heartbeats.
 */
public final class IngestorMetrics {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private final AtomicInteger connectionUp = new AtomicInteger();
    private final AtomicLong queueDepth = new AtomicLong();

    public IngestorMetrics() {
        registry.gauge("kraken.ingestor.ws.connection.up", connectionUp, AtomicInteger::get);
        registry.gauge("kraken.ingestor.queue.depth", queueDepth, AtomicLong::get);
    }

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    public void frameReceived(FrameClassifier.FrameType type) {
        registry.counter("kraken.ingestor.frames.received", "type", type.name()).increment();
    }

    public void framePublished(String topic) {
        registry.counter("kraken.ingestor.frames.published", "topic", topic).increment();
    }

    public void publishFailed(String topic) {
        registry.counter("kraken.ingestor.publish.failures", "topic", topic).increment();
    }

    /** A data frame whose symbol is not in the registry — we subscribed to it, so this is odd. */
    public void unknownSymbol(String symbol) {
        registry.counter("kraken.ingestor.unknown.symbol", "symbol", symbol).increment();
    }

    public void reconnected(String reason) {
        registry.counter("kraken.ingestor.ws.reconnects", "reason", reason).increment();
    }

    /**
     * A gap in {@code ingestSequence} became possible: a new connection started numbering
     * from zero. Consumers detect the gap themselves; this makes it countable centrally.
     */
    public void connectionEstablished() {
        registry.counter("kraken.ingestor.ws.connections").increment();
        connectionUp.set(1);
    }

    public void connectionLost() {
        connectionUp.set(0);
    }

    public void queueDepth(long depth) {
        queueDepth.set(depth);
    }
}
