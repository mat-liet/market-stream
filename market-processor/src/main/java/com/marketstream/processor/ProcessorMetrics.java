package com.marketstream.processor;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.KafkaStreams;

/**
 * What the processor reports about itself (design doc 21.1).
 *
 * <p>Two layers. Kafka Streams already publishes consumer lag, rebalance counts, dropped
 * records and state-restore times through its own metrics, so those are bridged wholesale
 * rather than reimplemented. On top of that sit the counters that are about this pipeline's
 * meaning rather than its plumbing: how many frames turned into trades, how many did not
 * and why, and how far behind the exchange the derived output is running.
 *
 * <p>{@code candles.emitted} is tagged by finality because provisional and final candles are
 * different products. Finals stalling while provisionals flow is the signature of a window
 * that never closed, and an untagged counter would hide it completely.
 */
public final class ProcessorMetrics implements AutoCloseable {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private final AtomicInteger running = new AtomicInteger();
    private final Timer endToEndLatency;

    private KafkaStreamsMetrics streamsMetrics;

    public ProcessorMetrics() {
        registry.gauge("market.processor.streams.running", running, AtomicInteger::get);
        endToEndLatency = Timer.builder("market.processor.end.to.end.latency")
                .description("processingTime - eventTime for an emitted candle")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    /** Bridges the Streams client's own metrics — lag, rebalances, dropped records. */
    public void bind(KafkaStreams streams) {
        streamsMetrics = new KafkaStreamsMetrics(streams);
        streamsMetrics.bindTo(registry);
    }

    public void streamsState(KafkaStreams.State state) {
        running.set(state == KafkaStreams.State.RUNNING ? 1 : 0);
    }

    public void tradeNormalized(String instrument) {
        registry.counter("market.processor.trades.normalized", "instrument", instrument).increment();
    }

    /** A frame we could categorise: it went to invalid.events with this reason. */
    public void rejected(String reason) {
        registry.counter("market.processor.invalid.events", "reason", reason).increment();
    }

    /** A frame we could not categorise at all: it went to dead-letter with its bytes. */
    public void deadLettered() {
        registry.counter("market.processor.dead.letter").increment();
    }

    /**
     * The exchange timestamp was missing or unbelievable and ingestion time was used
     * instead. Sustained nonzero means either Kraken changed its format or a clock is wrong,
     * and either way the windows are no longer where the exchange thinks they are.
     */
    public void eventTimeFallback(String instrument) {
        registry.counter("market.processor.event.time.fallback", "instrument", instrument).increment();
    }

    public void candleEmitted(String instrument, boolean isFinal) {
        registry.counter(
                        "market.processor.candles.emitted",
                        "instrument", instrument,
                        "final", Boolean.toString(isFinal))
                .increment();
    }

    public void endToEndLatency(Duration latency) {
        // Negative means the candle's window started after we emitted it, which cannot
        // happen; recording it would corrupt the histogram rather than reveal anything.
        if (!latency.isNegative()) {
            endToEndLatency.record(latency);
        }
    }

    @Override
    public void close() {
        if (streamsMetrics != null) {
            streamsMetrics.close();
        }
        registry.close();
    }
}
