package com.marketstream.sink;

import com.marketstream.avro.Candle;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Topics;
import com.marketstream.schemas.AvroSerdes;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point (design doc 6.3).
 *
 * <p>Two sinks, two consumer groups, two threads, one process. They share nothing but the
 * metrics registry and the HTTP server, which is what makes "candles stalled but trades kept
 * flowing" an outcome the deployment can actually have.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        SinkConfig config = SinkConfig.fromEnv();
        log.info("starting persistence-sink: bootstrap={} registry={} clickhouse={}",
                config.bootstrapServers(), config.schemaRegistryUrl(), config.clickHouseUrl());

        SinkMetrics metrics = new SinkMetrics();
        List<TopicSink<?>> sinks = new ArrayList<>();
        List<ClickHouseWriter<?>> writers = new ArrayList<>();

        sinks.add(build(
                Topics.NORMALIZED_TRADES, new TradesTable(), config.tradeBatchRows(),
                config, metrics, writers));
        sinks.add(build(
                Topics.DERIVED_CANDLES, new CandlesTable(), config.candleBatchRows(),
                config, metrics, writers));

        MetricsServer metricsServer = new MetricsServer(config.metricsPort(), metrics, sinks);

        List<Thread> threads = sinks.stream()
                .map(sink -> Thread.ofPlatform()
                        .name("sink-" + sink.tableName())
                        // Platform, not virtual: the loop blocks in poll() and in JDBC, both of
                        // which pin a carrier thread, so a virtual thread would buy nothing and
                        // cost the pinning warnings.
                        .unstarted(sink))
                .toList();

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            log.info("shutting down");
            // Loops first: each wakes out of poll() and closes its consumer without committing
            // whatever it was holding, so the batch replays and the idempotent write absorbs it.
            sinks.forEach(TopicSink::close);
            threads.forEach(thread -> {
                try {
                    thread.join(Duration.ofSeconds(10));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writers.forEach(ClickHouseWriter::close);
            metricsServer.close();
            metrics.close();
        }));

        metricsServer.start();
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static <V extends SpecificRecord> TopicSink<V> build(
            String topic,
            ClickHouseTable<V> table,
            int batchRows,
            SinkConfig config,
            SinkMetrics metrics,
            List<ClickHouseWriter<?>> writers) {

        KafkaConsumer<String, V> consumer = new KafkaConsumer<>(consumerConfig(config, table));
        ClickHouseWriter<V> writer = new ClickHouseWriter<>(config, table);
        writers.add(writer);
        metrics.bind(table.tableName(), consumer);
        // Registered before anything can pause, so the gauge exists in the very first scrape
        // rather than appearing only once something has gone wrong.
        metrics.paused(table.tableName(), false);
        return new TopicSink<>(topic, consumer, table, writer, metrics, batchRows, config);
    }

    private static Properties consumerConfig(SinkConfig config, ClickHouseTable<?> table) {
        Properties properties = new Properties();
        properties.putAll(AvroSerdes.config(config.schemaRegistryUrl()));
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        // One group per table, so the two loops' offsets and rebalances are independent.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                config.groupIdPrefix() + "-" + table.tableName().replace("market.", ""));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        // The processor writes under EOS v2. Without this the sink would happily persist
        // records from transactions that were later aborted — the single most damaging
        // misconfiguration available to this service.
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // Offsets advance only after ClickHouse has acked. Auto-commit would advance them on a
        // timer, past records still sitting in the buffer.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // A sink that skips history on first start is not a sink. Bounded by topic retention,
        // so the first-run backfill is at most a few days.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Never fetch more than one batch ahead: anything polled beyond the flush threshold is
        // held in memory with its offsets uncommitted, which is the buffering design doc 15.5
        // rules out.
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchRowsFor(config, table));
        return properties;
    }

    private static int batchRowsFor(SinkConfig config, ClickHouseTable<?> table) {
        return table instanceof CandlesTable ? config.candleBatchRows() : config.tradeBatchRows();
    }

    private Main() {
    }
}
