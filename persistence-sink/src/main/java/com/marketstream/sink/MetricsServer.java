package com.marketstream.sink;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes {@code /metrics} for Prometheus to scrape, and {@code /health} for Compose.
 *
 * <p>The port must match the {@code persistence-sink} target in
 * {@code infra/observability/prometheus.yml}, which already expects 9103.
 *
 * <p><strong>A ClickHouse outage is healthy.</strong> A paused sink is doing precisely what it
 * was designed to do — hold offsets and let lag grow rather than drop data — and failing the
 * health check would have Compose restart it into the same outage on a loop. Health here means
 * "both loops are alive"; whether they are making progress is what {@code market.sink.paused}
 * and consumer lag are for.
 */
public final class MetricsServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;

    public MetricsServer(int port, SinkMetrics metrics, List<TopicSink<?>> sinks) throws IOException {
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
            boolean healthy = sinks.stream().allMatch(TopicSink::isRunning);
            String report = sinks.stream()
                    .map(sink -> sink.tableName() + "=" + (sink.isRunning() ? "RUNNING" : "STOPPED"))
                    .collect(Collectors.joining(" "));
            byte[] body = report.getBytes(StandardCharsets.UTF_8);
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
