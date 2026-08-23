package com.marketstream.ingestor;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes {@code /metrics} for Prometheus to scrape.
 *
 * <p>The JDK's own {@link HttpServer} rather than a web framework: the ingestor serves one
 * endpoint to one scraper, and pulling in a framework for that would mean a second thread
 * pool, a second config surface and a second thing to keep patched in a service whose whole
 * point is to be boring.
 *
 * <p>The port must match the {@code kraken-ingestor} target in
 * {@code infra/observability/prometheus.yml}, which already expects 9101.
 */
public final class MetricsServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;

    public MetricsServer(int port, IngestorMetrics metrics) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.registry().scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // Liveness is deliberately separate from /metrics: a scrape failure and an unhealthy
        // process should not be the same signal.
        server.createContext("/health", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
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
