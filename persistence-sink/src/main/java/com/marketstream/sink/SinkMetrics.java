package com.marketstream.sink;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * What the sink reports about itself (design doc 21.1, the Sink row).
 *
 * <p>Every meter is tagged by {@code table}, because the two loops fail independently and an
 * untagged counter would make "candles are stuck" and "trades are stuck" indistinguishable —
 * which is the entire reason the sink runs two loops in the first place.
 *
 * <p>Consumer lag is not reimplemented here: {@link KafkaClientMetrics} bridges the client's
 * own {@code records-lag-max} and friends, which are the numbers the alert in design doc 21.4
 * actually watches.
 *
 * <p>Called from two sink threads, so the meter lookups are concurrency-safe.
 */
public final class SinkMetrics implements AutoCloseable {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private final Map<String, AtomicInteger> pausedByTable = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> batchSizeByTable = new ConcurrentHashMap<>();
    private final Map<String, Timer> flushLatencyByTable = new ConcurrentHashMap<>();
    private final List<KafkaClientMetrics> clientMetrics = new ArrayList<>();

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    /** Bridges one consumer's own metrics — lag above all. */
    public void bind(String table, Consumer<?, ?> consumer) {
        KafkaClientMetrics bridged = new KafkaClientMetrics(consumer, Tags.of("table", table));
        bridged.bindTo(registry);
        synchronized (clientMetrics) {
            clientMetrics.add(bridged);
        }
    }

    /**
     * A batch reached ClickHouse. Batch size is a distribution rather than a gauge because the
     * interesting question is whether inserts are large enough for ClickHouse to be happy with
     * them, and that is a shape, not a last value.
     */
    public void flushed(String table, int rows, Duration latency) {
        batchSizeByTable
                .computeIfAbsent(table, name -> DistributionSummary.builder("market.sink.batch.size")
                        .description("rows per ClickHouse insert")
                        .tag("table", name)
                        .publishPercentiles(0.5, 0.95)
                        .register(registry))
                .record(rows);
        flushLatencyByTable
                .computeIfAbsent(table, name -> Timer.builder("market.sink.flush.latency")
                        .description("time to write one batch, including retries")
                        .tag("table", name)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry))
                .record(latency);
        registry.counter("market.sink.rows.written", "table", table).increment(rows);
    }

    /** An insert failed. Sustained growth here is the ClickHouse-unavailable alert. */
    public void writeFailure(String table) {
        registry.counter("market.sink.db.write.failures", "table", table).increment();
    }

    /**
     * A record deliberately not stored in this phase — a provisional candle. Not an error, but
     * worth counting: if this stopped moving while candles kept arriving, the finality filter
     * would have inverted.
     */
    public void skipped(String table, String reason) {
        registry.counter("market.sink.records.skipped", "table", table, "reason", reason).increment();
    }

    /**
     * A record that could not be represented in the table and was dropped. Any nonzero value
     * is an alert: the sink has no ACL to route it anywhere, so this counter is the only trace
     * of it besides the log line.
     */
    public void rejected(String table, String reason) {
        registry.counter("market.sink.rows.rejected", "table", table, "reason", reason).increment();
    }

    /** 1 for exactly as long as this table's loop is backpressuring on an unreachable store. */
    public void paused(String table, boolean isPaused) {
        pausedByTable
                .computeIfAbsent(table, name -> registry.gauge(
                        "market.sink.paused", Tags.of("table", name), new AtomicInteger(), AtomicInteger::get))
                .set(isPaused ? 1 : 0);
    }

    @Override
    public void close() {
        synchronized (clientMetrics) {
            clientMetrics.forEach(KafkaClientMetrics::close);
        }
        registry.close();
    }
}
