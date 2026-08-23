package com.marketstream.ingestor;

import com.marketstream.common.Exchange;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point (design doc 6.1).
 *
 * <p>Startup is deliberately ordered so a misconfiguration fails before any data moves: the
 * instrument registry is read first, so an unreachable config store or an empty registry
 * stops the process here rather than after a connection is already streaming frames with
 * nowhere to route them.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        IngestorConfig config = IngestorConfig.fromEnv();
        log.info("starting kraken-ingestor: ws={} bootstrap={} registry={}",
                config.websocketUrl(), config.bootstrapServers(), config.schemaRegistryUrl());

        List<InstrumentRegistry.Instrument> instruments =
                InstrumentRegistry.loadEnabled(config, Exchange.KRAKEN);
        log.info("subscribing to {} instruments: {}",
                instruments.size(),
                instruments.stream().map(InstrumentRegistry.Instrument::exchangeSymbol).toList());

        IngestorMetrics metrics = new IngestorMetrics();
        MetricsServer metricsServer = new MetricsServer(config.metricsPort(), metrics);
        RawFramePublisher publisher = new RawFramePublisher(config, metrics);
        KrakenWebSocketClient client = new KrakenWebSocketClient(config);
        ConnectionSupervisor supervisor =
                new ConnectionSupervisor(config, metrics, publisher, client, instruments);

        Thread supervisorThread = Thread.ofPlatform().name("connection-supervisor").start(supervisor);

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            log.info("shutting down");
            // Order matters: stop reading, drain what is already queued, then release the
            // producer. Closing the producer first would fail the frames still in flight.
            supervisor.close();
            publisher.close();
            client.close();
            metricsServer.close();
        }));

        metricsServer.start();
        supervisorThread.join();
    }

    private Main() {
    }
}
