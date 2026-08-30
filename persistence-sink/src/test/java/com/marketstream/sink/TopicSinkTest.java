package com.marketstream.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.avro.Candle;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Decimals;
import com.marketstream.common.Topics;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The delivery guarantee, tested where it actually lives.
 *
 * <p>Everything the sink promises — never commit before ClickHouse acks, never drop, pause
 * rather than get evicted, never leave a topic uncommitted — is a property of this loop under
 * a failing write. That is precisely the condition a real ClickHouse will not produce on
 * demand, so the loop runs against a {@link MockConsumer} and a writer that fails when told.
 *
 * <p>{@code MockConsumer} is not thread-safe and the sink runs on its own thread, so every
 * interaction with it is queued through {@code schedulePollTask} and therefore executes inside
 * the sink's own {@code poll}.
 */
class TopicSinkTest {

    private static final TopicPartition TRADES = new TopicPartition(Topics.NORMALIZED_TRADES, 0);
    private static final TopicPartition CANDLES = new TopicPartition(Topics.DERIVED_CANDLES, 0);

    private final SinkMetrics metrics = new SinkMetrics();
    private final List<Running<?>> started = new ArrayList<>();

    @AfterEach
    void stopEverything() {
        started.forEach(Running::stop);
        metrics.close();
    }

    @Test
    void writesAndCommitsOnceTheBatchIsFull() {
        Running<TradeEvent> running = startTrades(2);
        offerTrades(running.consumer, 0, 1, 2);

        awaitUntil(() -> running.writer.batches.size() >= 2);

        // Two records fill the batch and go together; the third was still buffered at that
        // point and must not have been committed alongside them — it arrives in a batch of its
        // own once the time bound expires.
        assertThat(running.writer.batches.get(0)).hasSize(2);
        assertThat(running.writer.batches.get(1)).hasSize(1);
        assertThat(committed(running.consumer, TRADES)).isEqualTo(3L);
    }

    @Test
    void flushesAPartialBatchOnTheTimeBoundSoAQuietTopicIsNotHeldForever() {
        Running<TradeEvent> running = startTrades(1_000);
        offerTrades(running.consumer, 0);

        awaitUntil(() -> !running.writer.batches.isEmpty());

        assertThat(running.writer.batches.get(0)).hasSize(1);
        assertThat(committed(running.consumer, TRADES)).isEqualTo(1L);
    }

    @Test
    void doesNotCommitWhenTheWriteFails() {
        Running<TradeEvent> running = startTrades(1);
        running.writer.failing.set(true);
        offerTrades(running.consumer, 0);

        awaitUntil(() -> running.writer.attempts() >= 2);

        // The single most important assertion in the module: a failed write leaves the offset
        // where it was, so the record replays rather than vanishing.
        assertThat(committed(running.consumer, TRADES)).isEqualTo(-1L);
        assertThat(running.writer.batches).isEmpty();
    }

    @Test
    void pausesWhileTheStoreIsDownAndResumesWhenItReturns() {
        Running<TradeEvent> running = startTrades(1);
        running.writer.failing.set(true);
        offerTrades(running.consumer, 0);

        awaitUntil(() -> running.consumer.paused().contains(TRADES));

        running.writer.failing.set(false);

        awaitUntil(() -> !running.writer.batches.isEmpty());
        awaitUntil(() -> running.consumer.paused().isEmpty());
        assertThat(committed(running.consumer, TRADES)).isEqualTo(1L);
    }

    @Test
    void commitsSkippedRecordsWithoutWritingThem() {
        // derived.candles is mostly provisional records. If the commit were keyed off the
        // buffer alone, a stretch of nothing but skipped records would never advance the
        // offset, and every restart would replay the topic from the start of retention.
        Running<Candle> running = start(
                Topics.DERIVED_CANDLES, CANDLES, new CandlesTable(),
                new MockConsumer<>(OffsetResetStrategy.EARLIEST), 10);
        running.consumer.schedulePollTask(() -> running.consumer.addRecord(new ConsumerRecord<>(
                Topics.DERIVED_CANDLES, 0, 0L, "KRAKEN|BTC/USD", Records.candle(false))));

        awaitUntil(() -> committed(running.consumer, CANDLES) == 1L);

        assertThat(running.writer.batches).isEmpty();
    }

    // ------------------------------------------------------------------ harness ----

    private record Running<V extends SpecificRecord>(
            MockConsumer<String, V> consumer, StubWriter<V> writer, TopicSink<V> sink, Thread thread) {

        void stop() {
            sink.close();
            try {
                thread.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Running<TradeEvent> startTrades(int batchRows) {
        return start(
                Topics.NORMALIZED_TRADES, TRADES, new TradesTable(),
                new MockConsumer<>(OffsetResetStrategy.EARLIEST), batchRows);
    }

    private <V extends SpecificRecord> Running<V> start(
            String topic,
            TopicPartition partition,
            ClickHouseTable<V> table,
            MockConsumer<String, V> consumer,
            int batchRows) {

        // Queued rather than applied directly: the sink calls subscribe() on its own thread,
        // and an assignment made before that would be wiped by it.
        consumer.schedulePollTask(() -> {
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.rebalance(List.of(partition));
        });
        StubWriter<V> writer = new StubWriter<>();
        TopicSink<V> sink = new TopicSink<>(topic, consumer, table, writer, metrics, batchRows, config());
        Thread thread = Thread.ofPlatform().name("test-sink-" + table.tableName()).start(sink);
        Running<V> running = new Running<>(consumer, writer, sink, thread);
        started.add(running);
        return running;
    }

    private void offerTrades(MockConsumer<String, TradeEvent> consumer, long... offsets) {
        for (long offset : offsets) {
            consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(
                    Topics.NORMALIZED_TRADES, 0, offset, "KRAKEN|BTC/USD",
                    Records.trade(Long.toString(offset), Decimals.parse("1")))));
        }
    }

    /** @return the committed offset, or -1 if nothing has been committed */
    private static long committed(MockConsumer<String, ?> consumer, TopicPartition partition) {
        OffsetAndMetadata offset = consumer.committed(Set.of(partition)).get(partition);
        return offset == null ? -1L : offset.offset();
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting", e);
            }
        }
        throw new AssertionError("condition did not become true within 10s");
    }

    private static SinkConfig config() {
        return new SinkConfig(
                "unused", "unused", "unused", "unused", "unused", "test",
                0,
                1_000,
                1_000,
                // Short enough that the time-bound flush is observable inside a test, long
                // enough that the batch-size flush is still what triggers first when it can.
                Duration.ofMillis(300),
                Duration.ofMillis(20),
                Duration.ofMillis(20),
                Duration.ofMillis(50));
    }

    private static final class StubWriter<V extends SpecificRecord> implements BatchWriter<V> {

        final List<List<V>> batches = new CopyOnWriteArrayList<>();
        final AtomicBoolean failing = new AtomicBoolean();

        private final List<Long> attemptTimes = new CopyOnWriteArrayList<>();

        @Override
        public void write(List<V> rows) throws SQLException {
            attemptTimes.add(System.nanoTime());
            if (failing.get()) {
                throw new SQLException("ClickHouse is pretending to be down");
            }
            batches.add(new ArrayList<>(rows));
        }

        int attempts() {
            return attemptTimes.size();
        }
    }
}
