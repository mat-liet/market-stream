package com.marketstream.sink;

import com.marketstream.common.Backoff;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One topic, one consumer group, one table: poll, batch, write, then commit.
 *
 * <p>Generic over the record type so both tables share one loop. Each instance owns its
 * consumer, its buffer and its backpressure state, so a stall writing candles cannot stall
 * trades — the reason the sink runs two of these rather than one consumer subscribed to both
 * topics.
 *
 * <p><strong>Offsets commit only after ClickHouse acks</strong> (design doc 9). That ordering
 * is the entire delivery guarantee: a crash between the ack and the commit replays the batch,
 * and the {@code ReplacingMergeTree} key collapses it, which is what "at-least-once plus
 * idempotent equals effectively-once" means in practice (design doc 19.2). Committing first
 * would turn every crash into silent data loss, which is why auto-commit is off.
 *
 * <p>At commit time the consumer's position is exactly the end of what has been buffered —
 * nothing is polled between the decision to flush and the commit — so a plain
 * {@code commitSync()} commits precisely the batch that was just written, and no more.
 */
final class TopicSink<V extends SpecificRecord> implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicSink.class);

    private final String topic;
    private final Consumer<String, V> consumer;
    private final ClickHouseTable<V> table;
    private final BatchWriter<V> writer;
    private final SinkMetrics metrics;
    private final int batchRows;
    private final Duration flushInterval;
    private final Duration pollTimeout;
    private final Backoff backoff;

    private final List<V> buffer = new ArrayList<>();

    private volatile boolean running = true;
    private boolean paused;
    private long lastFlushNanos = System.nanoTime();

    /**
     * Records consumed since the last commit, buffered or not.
     *
     * <p>Counted separately from the buffer because most of {@code derived.candles} is
     * provisional and never buffered. Keying the commit off the buffer alone would leave a
     * topic of nothing but skipped records permanently uncommitted, and every restart would
     * replay it from the start of retention.
     */
    private int pendingRecords;

    TopicSink(
            String topic,
            Consumer<String, V> consumer,
            ClickHouseTable<V> table,
            BatchWriter<V> writer,
            SinkMetrics metrics,
            int batchRows,
            SinkConfig config) {

        this.topic = topic;
        this.consumer = consumer;
        this.table = table;
        this.writer = writer;
        this.metrics = metrics;
        this.batchRows = batchRows;
        this.flushInterval = config.flushInterval();
        this.pollTimeout = config.pollTimeout();
        this.backoff = new Backoff(config.initialRetryBackoff(), config.maxRetryBackoff());
    }

    @Override
    public void run() {
        log.info("sinking {} -> {} (batch {} rows or {})",
                topic, table.tableName(), batchRows, flushInterval);
        try {
            consumer.subscribe(List.of(topic));
            while (running) {
                ConsumerRecords<String, V> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, V> record : records) {
                    accept(record.value());
                }
                if (flushDue()) {
                    flushAndCommit();
                }
            }
        } catch (WakeupException e) {
            // The documented way to break out of poll() on shutdown, not a failure.
            log.info("{} sink woken for shutdown", table.tableName());
        } catch (RuntimeException e) {
            log.error("{} sink stopped on an unrecoverable error", table.tableName(), e);
            throw e;
        } finally {
            running = false;
            // No last-gasp flush. Whatever is buffered was never committed, so it replays on
            // the next start and the idempotent write absorbs it. Flushing here would let a
            // ClickHouse outage hold shutdown open for as long as the outage lasts.
            consumer.close();
            log.info("{} sink stopped with {} unwritten records, which will replay",
                    table.tableName(), buffer.size());
        }
    }

    private void accept(V value) {
        pendingRecords++;
        if (value == null) {
            // A tombstone on a non-compacted topic is not something this pipeline produces.
            metrics.rejected(table.tableName(), "null value");
            return;
        }
        switch (table.inspect(value)) {
            case ClickHouseTable.Verdict.Store ignored -> buffer.add(value);
            case ClickHouseTable.Verdict.Skip skip -> metrics.skipped(table.tableName(), skip.reason());
            case ClickHouseTable.Verdict.Reject reject -> {
                // Logged whole: this is the only record of a row the platform decided not to
                // keep, and the sink has no ACL to write it anywhere else (design doc 22).
                log.warn("dropping a record {} cannot store ({}): {}",
                        table.tableName(), reject.reason(), value);
                metrics.rejected(table.tableName(), reject.reason());
            }
        }
    }

    private boolean flushDue() {
        if (pendingRecords == 0) {
            // Nothing to write and nothing to commit. The clock resets so that a quiet topic's
            // next record does not arrive already overdue and insert a batch of one.
            lastFlushNanos = System.nanoTime();
            return false;
        }
        return buffer.size() >= batchRows
                || System.nanoTime() - lastFlushNanos >= flushInterval.toNanos();
    }

    /**
     * Writes the buffer, retrying indefinitely, then commits.
     *
     * <p>While retrying, the assignment is <em>paused</em> and the loop keeps polling. A paused
     * poll fetches nothing but still heartbeats, so the consumer stays in its group instead of
     * being evicted by {@code max.poll.interval.ms} into a rebalance loop — and pausing is
     * literally what design doc 15.5 asks for: stop committing offsets and stop consuming, let
     * lag grow in Kafka, never buffer beyond the batch in hand, never drop.
     */
    private void flushAndCommit() {
        long started = System.nanoTime();
        while (running) {
            try {
                if (!buffer.isEmpty()) {
                    writer.write(buffer);
                }
                resumeIfPaused();
                backoff.reset();
                consumer.commitSync();
                if (!buffer.isEmpty()) {
                    metrics.flushed(
                            table.tableName(),
                            buffer.size(),
                            Duration.ofNanos(System.nanoTime() - started));
                }
                buffer.clear();
                pendingRecords = 0;
                lastFlushNanos = System.nanoTime();
                return;
            } catch (SQLException e) {
                metrics.writeFailure(table.tableName());
                pause();
                Duration delay = backoff.nextDelay();
                log.warn("writing {} rows to {} failed (attempt {}), retrying in {}: {}",
                        buffer.size(), table.tableName(), backoff.attempt(), delay, e.toString());
                heartbeatFor(delay);
            }
        }
    }

    /**
     * Waits out the backoff, spending as much of it as possible inside {@code poll} so the wait
     * doubles as a heartbeat and stays interruptible by {@link #close()}.
     *
     * <p>The remaining sleep exists because {@code poll} is only guaranteed to return
     * <em>within</em> its timeout, not to consume it: a metadata refresh or a rebalance can end
     * it early. Without the sleep an early return would turn the backoff into a spin, retrying
     * a failed write thousands of times a second for as long as the outage lasted.
     */
    private void heartbeatFor(Duration delay) {
        long deadline = System.nanoTime() + delay.toNanos();
        ConsumerRecords<String, V> records = consumer.poll(delay);
        if (!records.isEmpty()) {
            // Only reachable if a rebalance handed us partitions after the pause. Buffering
            // them is correct: they sit behind the same uncommitted offsets.
            records.forEach(record -> accept(record.value()));
        }
        long remaining = deadline - System.nanoTime();
        if (remaining > 0 && running) {
            try {
                Thread.sleep(Duration.ofNanos(remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }

    private void pause() {
        // Re-pauses on every failure rather than once: a rebalance during the outage assigns
        // partitions in the resumed state, and one unpaused partition would defeat the whole
        // mechanism.
        consumer.pause(consumer.assignment());
        if (!paused) {
            paused = true;
            metrics.paused(table.tableName(), true);
            log.warn("{} paused: ClickHouse is unreachable, lag will grow until it returns",
                    table.tableName());
        }
    }

    private void resumeIfPaused() {
        if (!paused) {
            return;
        }
        consumer.resume(consumer.assignment());
        paused = false;
        metrics.paused(table.tableName(), false);
        log.info("{} resumed: ClickHouse accepted a batch again", table.tableName());
    }

    boolean isRunning() {
        return running;
    }

    String tableName() {
        return table.tableName();
    }

    @Override
    public void close() {
        running = false;
        consumer.wakeup();
    }
}
