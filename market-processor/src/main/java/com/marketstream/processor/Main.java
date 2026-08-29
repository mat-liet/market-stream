package com.marketstream.processor;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point (design doc 6.2).
 *
 * <p>Startup is ordered so a misconfiguration fails before any data moves: the processor
 * version is read from the registry first, so an unreachable config store or an
 * unregistered build stops the process here rather than after candles are already being
 * stamped with a version nobody can trace.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ProcessorConfig config = ProcessorConfig.fromEnv();
        log.info("starting market-processor: applicationId={} bootstrap={} registry={}",
                config.applicationId(), config.bootstrapServers(), config.schemaRegistryUrl());

        String processorVersion = ProcessorVersionRegistry.loadLiveVersion(config);
        log.info("processor version {} (window={} grace={})",
                processorVersion, config.candleWindow(), config.candleGrace());

        ProcessorMetrics metrics = new ProcessorMetrics();
        KafkaStreams streams = new KafkaStreams(
                ProcessorTopology.build(config, processorVersion, metrics, Instant::now),
                StreamsSettings.forProcessor(config));

        metrics.bind(streams);
        MetricsServer metricsServer = new MetricsServer(config.metricsPort(), metrics, streams::state);

        // Held until Streams reaches a terminal state, so the process exits when the
        // topology dies rather than lingering as a healthy-looking container doing nothing.
        CountDownLatch stopped = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            log.info("streams state {} -> {}", oldState, newState);
            metrics.streamsState(newState);
            if (newState.hasCompletedShutdown() || newState == KafkaStreams.State.ERROR) {
                stopped.countDown();
            }
        });
        streams.setUncaughtExceptionHandler(error -> {
            log.error("uncaught error on a stream thread; replacing it", error);
            // Replace the thread rather than kill the client: a single-instance deployment
            // taking the whole topology down over one thread's failure is a worse outcome
            // than losing that thread's task briefly. EOS has already rolled back whatever
            // it was mid-way through.
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            log.info("shutting down");
            // Streams first so the in-flight transaction commits or aborts cleanly, then the
            // things that only observe it.
            streams.close();
            metricsServer.close();
            metrics.close();
            stopped.countDown();
        }));

        metricsServer.start();
        streams.start();
        stopped.await();
    }

    private Main() {
    }
}
