package com.marketstream.ingestor;

import com.marketstream.common.Backoff;
import com.marketstream.common.Exchange;
import com.marketstream.common.InstrumentKey;
import com.marketstream.common.Topics;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps one healthy Kraken connection alive and everything it delivers moving to Kafka.
 *
 * <p>Three threads, each with one job:
 *
 * <ol>
 *   <li>the supervisor thread runs the connect → subscribe → watch → reconnect loop;
 *   <li>a per-connection handoff thread classifies frames and puts them on the queue,
 *       blocking when it is full — this is where backpressure originates;
 *   <li>the publisher thread drains the queue into {@link RawFramePublisher}.
 * </ol>
 *
 * <p><strong>On the watchdog:</strong> it triggers on silence of any kind, not on missing
 * heartbeats. Kraken only sends heartbeats when a connection is otherwise idle, so a busy
 * connection legitimately shows none — a heartbeat-only watchdog would tear down exactly the
 * connections that are working hardest. Every frame, of every type, counts as liveness.
 *
 * <p><strong>On {@code ingestSequence}:</strong> it is scoped to a connection and restarts at
 * zero with each one, which is why {@code sourceConnectionId} travels beside it. A consumer
 * seeing the sequence restart knows the connection was replaced and that frames may be
 * missing across the boundary — without needing any cooperation from the exchange.
 */
public final class ConnectionSupervisor implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionSupervisor.class);

    /** One frame on its way to Kafka. */
    private record Pending(
            String topic,
            InstrumentKey key,
            String channel,
            byte[] payload,
            String connectionId,
            long ingestSequence) {
    }

    private static final Pending POISON = new Pending(null, null, null, null, null, -1);

    private final IngestorConfig config;
    private final IngestorMetrics metrics;
    private final RawFramePublisher publisher;
    private final KrakenWebSocketClient client;
    private final FrameClassifier classifier = new FrameClassifier();

    private final List<String> wireSymbols;
    private final Map<String, String> canonicalByWireSymbol;

    private final BlockingQueue<Pending> queue;
    private final Backoff backoff;

    private final AtomicLong ingestSequence = new AtomicLong();
    private final AtomicReference<String> connectionId = new AtomicReference<>();
    private final AtomicReference<Instant> lastFrameAt = new AtomicReference<>(Instant.EPOCH);

    private volatile boolean running = true;
    private volatile WebSocket webSocket;
    private Thread publisherThread;

    public ConnectionSupervisor(
            IngestorConfig config,
            IngestorMetrics metrics,
            RawFramePublisher publisher,
            KrakenWebSocketClient client,
            List<InstrumentRegistry.Instrument> instruments) {

        this.config = config;
        this.metrics = metrics;
        this.publisher = publisher;
        this.client = client;
        this.wireSymbols = instruments.stream().map(InstrumentRegistry.Instrument::exchangeSymbol).toList();
        this.canonicalByWireSymbol = InstrumentRegistry.byExchangeSymbol(instruments);
        this.queue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.backoff = new Backoff(config.initialBackoff(), config.maxBackoff());
    }

    @Override
    public void run() {
        publisherThread = Thread.ofPlatform().name("raw-frame-publisher").start(this::drainQueue);
        while (running) {
            try {
                runOneConnection();
            } catch (Exception e) {
                log.warn("connection attempt {} failed", backoff.attempt(), e);
            }
            if (!running) {
                break;
            }
            Duration delay = backoff.nextDelay();
            log.info("reconnecting in {} ms", delay.toMillis());
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("supervisor stopped");
    }

    private void runOneConnection() {
        CountDownLatch disconnected = new CountDownLatch(1);
        // Per connection so that shutting it down cannot strand a frame from a previous one.
        ExecutorService handoff = Executors.newSingleThreadExecutor(
                r -> Thread.ofPlatform().name("ws-handoff").unstarted(r));

        ingestSequence.set(0);
        connectionId.set(null);
        lastFrameAt.set(Instant.now());
        Instant connectedAt = Instant.now();

        try {
            webSocket = client.connect(new FrameAssemblingListener(this::onFrame, handoff, disconnected::countDown));
            metrics.connectionEstablished();
            client.subscribeTrades(webSocket, wireSymbols);
            client.subscribeBook(webSocket, wireSymbols);
            watchUntilDisconnected(disconnected);
        } finally {
            metrics.connectionLost();
            WebSocket current = webSocket;
            webSocket = null;
            if (current != null) {
                current.abort();
            }
            drainHandoff(handoff);
            // Only forgive the backoff for a connection that actually stayed up. Resetting
            // on every successful handshake turns a connection that dies immediately after
            // connecting into a reconnect storm.
            if (Duration.between(connectedAt, Instant.now()).compareTo(config.silenceTimeout()) > 0) {
                backoff.reset();
            }
        }
    }

    /**
     * Lets frames already taken off the socket finish reaching the queue.
     *
     * <p>{@code shutdownNow} would be wrong here: it cancels queued handoff tasks, and each
     * one holds a frame Kraken has already delivered. Dropping those on a reconnect is
     * exactly the silent loss this service exists to avoid — and it would happen precisely
     * during the incidents where the data matters most. The wait is bounded so a handoff
     * blocked on an unreachable Kafka cannot stop the reconnect indefinitely; if it expires
     * the loss becomes visible instead of silent.
     */
    private void drainHandoff(ExecutorService handoff) {
        handoff.shutdown();
        try {
            if (!handoff.awaitTermination(10, TimeUnit.SECONDS)) {
                List<Runnable> abandoned = handoff.shutdownNow();
                if (!abandoned.isEmpty()) {
                    log.error("dropped {} received frames: handoff did not drain in 10s", abandoned.size());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handoff.shutdownNow();
        }
    }

    private void watchUntilDisconnected(CountDownLatch disconnected) {
        while (running) {
            try {
                if (disconnected.await(1, TimeUnit.SECONDS)) {
                    metrics.reconnected("closed");
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            // Silence we caused ourselves is not evidence of a dead connection. When the
            // queue is full the handoff thread is blocked, the socket is deliberately not
            // being read, and no frame can arrive by construction — so the watchdog would
            // otherwise diagnose the backpressure it asked for as a failure and reconnect
            // into it, repeatedly, for as long as the outage lasts. Each of those reconnects
            // restarts ingestSequence, re-requests book snapshots, and hammers the exchange
            // at its least convenient moment. Hold the clock instead, and let the watchdog
            // resume judging the connection once we are reading again.
            if (queue.remainingCapacity() == 0) {
                lastFrameAt.set(Instant.now());
                continue;
            }
            Duration silence = Duration.between(lastFrameAt.get(), Instant.now());
            if (silence.compareTo(config.silenceTimeout()) > 0) {
                log.warn("no frame of any kind for {} ms, treating the connection as dead", silence.toMillis());
                metrics.reconnected("silence");
                return;
            }
        }
    }

    /** Runs on the handoff thread. Blocking here is what throttles the socket. */
    private void onFrame(byte[] frame) {
        lastFrameAt.set(Instant.now());
        FrameClassifier.Frame classified = classifier.classify(frame);
        metrics.frameReceived(classified.type());

        switch (classified.type()) {
            case STATUS -> {
                if (classified.connectionId() != null) {
                    connectionId.set(classified.connectionId());
                    log.info("kraken connection id {}", classified.connectionId());
                }
            }
            case SUBSCRIBE_ACK -> log.info("subscription response: {}", text(frame));
            case HEARTBEAT -> {
                // Liveness only, already recorded above.
            }
            case TRADE_DATA, BOOK_DATA -> enqueue(topicFor(classified), classified, frame);
            case UNKNOWN, MALFORMED -> {
                // Real bytes from the exchange that we could not route. Keeping them is the
                // difference between "we do not understand this" and "we lost this".
                log.warn("unroutable {} frame: {}", classified.type(), text(frame));
                enqueue(Topics.DEAD_LETTER, classified, frame);
            }
        }
    }

    private static String topicFor(FrameClassifier.Frame frame) {
        return frame.type() == FrameClassifier.FrameType.TRADE_DATA
                ? Topics.RAW_KRAKEN_TRADE
                : Topics.RAW_KRAKEN_BOOK;
    }

    private void enqueue(String topic, FrameClassifier.Frame classified, byte[] frame) {
        InstrumentKey key = keyFor(topic, classified);
        String channel = classified.channel() == null ? "unknown" : classified.channel();
        Pending pending = new Pending(
                topic, key, channel, frame, connectionId(), ingestSequence.getAndIncrement());
        try {
            queue.put(pending);
            metrics.queueDepth(queue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * The partition key, or null when there is nothing safe to key on.
     *
     * <p>A data frame naming a symbol we did not subscribe to should be impossible. If one
     * arrives, it is routed to {@code dead-letter} rather than guessed at: inventing a
     * canonical symbol would put a record on a data topic under a key nothing downstream
     * recognises, which is harder to notice than an empty dead-letter topic that suddenly
     * is not empty.
     */
    private InstrumentKey keyFor(String topic, FrameClassifier.Frame classified) {
        if (Topics.DEAD_LETTER.equals(topic)) {
            return null;
        }
        String wireSymbol = classified.symbol();
        if (wireSymbol == null) {
            metrics.unknownSymbol("none");
            log.warn("{} frame carried no symbol", classified.type());
            return null;
        }
        String canonical = canonicalByWireSymbol.get(wireSymbol.toUpperCase());
        if (canonical == null) {
            metrics.unknownSymbol(wireSymbol);
            log.warn("symbol '{}' is not in the instrument registry", wireSymbol);
            return null;
        }
        return InstrumentKey.of(Exchange.KRAKEN, canonical);
    }

    /** Falls back to a local id only for frames that beat the status message to us. */
    private String connectionId() {
        String id = connectionId.get();
        return id == null ? "pending" : id;
    }

    private void drainQueue() {
        while (true) {
            Pending pending;
            try {
                pending = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (pending == POISON) {
                break;
            }
            metrics.queueDepth(queue.size());
            // A frame that could not be keyed still gets stored; only the topic differs.
            String topic = pending.key() == null && !Topics.DEAD_LETTER.equals(pending.topic())
                    ? Topics.DEAD_LETTER
                    : pending.topic();
            publisher.publish(
                    topic,
                    pending.key(),
                    pending.channel(),
                    pending.payload(),
                    pending.connectionId(),
                    pending.ingestSequence());
        }
        publisher.flush();
        log.info("publisher drained");
    }

    private static String text(byte[] frame) {
        String value = new String(frame, StandardCharsets.UTF_8);
        return value.length() <= 500 ? value : value.substring(0, 500) + "…";
    }

    @Override
    public void close() {
        running = false;
        WebSocket current = webSocket;
        if (current != null) {
            current.abort();
        }
        if (publisherThread != null) {
            // Let whatever is already queued reach Kafka rather than discarding it. Nothing
            // is producing any more, so the drain always makes room for the sentinel.
            try {
                while (publisherThread.isAlive() && !queue.offer(POISON, 1, TimeUnit.SECONDS)) {
                    log.info("waiting for {} queued frames to publish", queue.size());
                }
                publisherThread.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
