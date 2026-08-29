package com.marketstream.processor;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes {@code /metrics} for Prometheus to scrape, and {@code /health} for Compose.
 *
 * <p>The JDK's own {@link HttpServer} rather than a web framework, for the same reason as
 * the ingestor's: two endpoints do not justify a second thread pool and a second thing to
 * keep patched.
 *
 * <p>The port must match the {@code market-processor} target in
 * {@code infra/observability/prometheus.yml}, which already expects 9102.
 */
public final class MetricsServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;

    /**
     * @param state the Streams state, consulted per request. Health is unlike the
     *              ingestor's: a Streams app can be alive and not processing — rebalancing,
     *              restoring state, or dead after an uncaught exception — and reporting
     *              those as healthy would let Compose call a stalled processor fine.
     */
    public MetricsServer(int port, ProcessorMetrics metrics, Supplier<KafkaStreams.State> state)
            throws IOException {

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.registry().scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            KafkaStreams.State current = state.get();
            // REBALANCING is deliberately healthy: it is a normal, transient phase of a
            // working app, and failing health there would restart it into another rebalance.
            boolean healthy = current == KafkaStreams.State.RUNNING
                    || current == KafkaStreams.State.REBALANCING;
            byte[] body = current.name().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(healthy ? 200 : 503, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public void start() {
        server.start();
        log.info("metrics listening on http://0.0.0.0:{}/metrics", server.getAddress().getPort());
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
