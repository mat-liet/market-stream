package com.marketstream.processor;

import com.marketstream.avro.Candle;
import com.marketstream.avro.EventTimeSource;
import com.marketstream.avro.InvalidEvent;
import com.marketstream.avro.RawEnvelope;
import com.marketstream.avro.TradeEvent;
import com.marketstream.common.Topics;
import com.marketstream.schemas.AvroSerdes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * The phase 1 topology: raw frames in, canonical trades and 1-minute candles out.
 *
 * <p>Two sub-topologies in one application, joined by the real {@code normalized.trades}
 * topic rather than an internal repartition. That is deliberate: {@code normalized.trades}
 * is a published contract other services read (design doc 22), and materialising it means
 * the aggregation can be reasoned about, replayed and tested from a topic that exists.
 *
 * <p>One {@code application.id} for both gives them a shared transactional context, so the
 * offset commit, the normalised trade and the candle it produced all land or none do
 * (design doc 6.2).
 */
public final class ProcessorTopology {

    /** Matches the store name in the design doc's state table (13.3). */
    static final String CANDLE_STORE = "candle-1m";

    private ProcessorTopology() {
    }

    /**
     * @param clock supplies {@code processingTime}. Injected rather than called directly so
     *              the fixture test can pin it and compare whole records; it is the only
     *              non-deterministic input to the derived output.
     */
    public static Topology build(
            ProcessorConfig config,
            String processorVersion,
            ProcessorMetrics metrics,
            Supplier<Instant> clock) {

        StreamsBuilder builder = new StreamsBuilder();
        String registry = config.schemaRegistryUrl();

        Serde<String> stringSerde = Serdes.String();
        Serde<RawEnvelope> rawSerde = AvroSerdes.forValue(registry);
        Serde<TradeEvent> tradeSerde = AvroSerdes.forValue(registry);
        Serde<Candle> candleSerde = AvroSerdes.forValue(registry);
        Serde<InvalidEvent> invalidSerde = AvroSerdes.forValue(registry);

        normalize(builder, config, processorVersion, metrics,
                stringSerde, rawSerde, tradeSerde, invalidSerde);
        aggregate(builder, config, processorVersion, metrics, clock,
                stringSerde, tradeSerde, candleSerde);

        return builder.build();
    }

    /** raw.kraken.trade -> normalized.trades, with the two failure paths beside it. */
    private static void normalize(
            StreamsBuilder builder,
            ProcessorConfig config,
            String processorVersion,
            ProcessorMetrics metrics,
            Serde<String> stringSerde,
            Serde<RawEnvelope> rawSerde,
            Serde<TradeEvent> tradeSerde,
            Serde<InvalidEvent> invalidSerde) {

        TradeNormalizer normalizer = new TradeNormalizer(processorVersion);

        KStream<String, NormalizationResult> results = builder
                .stream(Topics.RAW_KRAKEN_TRADE,
                        Consumed.with(stringSerde, rawSerde)
                                .withTimestampExtractor(new ReceivedAtTimestampExtractor())
                                .withName("raw-trades"))
                // One frame can carry several trades, and each needs its own key: the frame
                // is single-symbol today but nothing in the wire format promises that.
                .flatMap((key, envelope) -> {
                    List<NormalizationResult> outcomes = normalizer.normalize(key, envelope);
                    List<KeyValue<String, NormalizationResult>> keyed =
                            new ArrayList<>(outcomes.size());
                    for (NormalizationResult outcome : outcomes) {
                        keyed.add(KeyValue.pair(outcome.key(), outcome));
                    }
                    return keyed;
                }, Named.as("normalize"));

        Map<String, KStream<String, NormalizationResult>> routes = results
                .split(Named.as("route-"))
                .branch((key, value) -> value instanceof NormalizationResult.Normalized,
                        Branched.as("trade"))
                .branch((key, value) -> value instanceof NormalizationResult.Rejected,
                        Branched.as("invalid"))
                // Unparseable is the only case left, but a default branch rather than a
                // third predicate means a future result type cannot fall off the end of the
                // topology and vanish.
                .defaultBranch(Branched.as("dead"));

        routes.get("route-trade")
                .mapValues(value -> ((NormalizationResult.Normalized) value).trade(),
                        Named.as("unwrap-trade"))
                .peek((key, trade) -> {
                    metrics.tradeNormalized(trade.getHeader().getInstrument());
                    if (trade.getHeader().getEventTimeSource() == EventTimeSource.INGESTION_FALLBACK) {
                        metrics.eventTimeFallback(trade.getHeader().getInstrument());
                    }
                }, Named.as("count-trade"))
                .to(Topics.NORMALIZED_TRADES, Produced.with(stringSerde, tradeSerde));

        routes.get("route-invalid")
                .mapValues(value -> ((NormalizationResult.Rejected) value).invalid(),
                        Named.as("unwrap-invalid"))
                .peek((key, invalid) -> metrics.rejected(invalid.getReason().name()),
                        Named.as("count-invalid"))
                .to(Topics.INVALID_EVENTS, Produced.with(stringSerde, invalidSerde));

        routes.get("route-dead")
                // The envelope goes on verbatim. dead-letter's registry subject is already
                // bound to RawEnvelope by the ingestor, and forwarding the original keeps
                // sourceConnectionId and ingestSequence — which say exactly which connection
                // delivered the bad frame and where in its stream it sat.
                .mapValues(value -> ((NormalizationResult.Unparseable) value).envelope(),
                        Named.as("unwrap-dead"))
                .peek((key, envelope) -> metrics.deadLettered(), Named.as("count-dead"))
                .to(Topics.DEAD_LETTER, Produced.with(stringSerde, rawSerde));
    }

    /** normalized.trades -> derived.candles, provisional and final. */
    private static void aggregate(
            StreamsBuilder builder,
            ProcessorConfig config,
            String processorVersion,
            ProcessorMetrics metrics,
            Supplier<Instant> clock,
            Serde<String> stringSerde,
            Serde<TradeEvent> tradeSerde,
            Serde<Candle> candleSerde) {

        KTable<Windowed<String>, Candle> windows = builder
                .stream(Topics.NORMALIZED_TRADES,
                        Consumed.with(stringSerde, tradeSerde)
                                .withTimestampExtractor(new EventTimeTimestampExtractor())
                                .withName("normalized-trades"))
                // Already keyed by exchange|instrument upstream, so this groups without a
                // repartition — which is the point of keying every data topic the same way.
                .groupByKey(Grouped.with(stringSerde, tradeSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(config.candleWindow(), config.candleGrace()))
                .aggregate(
                        CandleAggregator::empty,
                        (key, trade, accumulator) -> CandleAggregator.fold(accumulator, trade),
                        Materialized.<String, Candle, WindowStore<Bytes, byte[]>>as(CANDLE_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(candleSerde));

        // Provisional: re-emitted every commit interval while the window is open. Explicitly
        // non-authoritative, and the sink drops it — it exists so a live view has something
        // to show before the window closes (design doc 13.1).
        windows.toStream(Named.as("provisional-candles"))
                .map((window, candle) -> stamp(
                                window, candle, false, processorVersion, metrics, clock),
                        Named.as("stamp-provisional"))
                .to(Topics.DERIVED_CANDLES, Produced.with(stringSerde, candleSerde));

        // Final: emitted once, after the window closes and grace elapses, and immutable from
        // then on (correctness invariant 1).
        //
        // This is stream-time driven, which is the honest trade-off. The final candle for a
        // window appears only when a later trade advances stream time past windowEnd+grace,
        // so a quiet instrument's last window stays unfinalised until it trades again. A
        // wall-clock punctuator would close it promptly and make the output depend on when
        // the job ran, which breaks reproducibility (invariant 6) and would leave the
        // deterministic fixture test unable to pass. Correct and late beats timely and
        // irreproducible.
        windows.suppress(Suppressed.untilWindowCloses(
                        Suppressed.BufferConfig.maxBytes(config.suppressBufferBytes())
                                .shutDownWhenFull()))
                .toStream(Named.as("final-candles"))
                .map((window, candle) -> stamp(
                                window, candle, true, processorVersion, metrics, clock),
                        Named.as("stamp-final"))
                .to(Topics.DERIVED_CANDLES, Produced.with(stringSerde, candleSerde));
    }

    private static KeyValue<String, Candle> stamp(
            Windowed<String> window,
            Candle accumulator,
            boolean isFinal,
            String processorVersion,
            ProcessorMetrics metrics,
            Supplier<Instant> clock) {

        Instant now = clock.get();
        Candle candle = CandleAggregator.stamp(
                accumulator,
                window.key(),
                window.window().startTime(),
                window.window().endTime(),
                isFinal,
                processorVersion,
                now);

        metrics.candleEmitted(candle.getHeader().getInstrument(), isFinal);
        if (isFinal) {
            // Measured from the window's end, not its start. Starting the clock at
            // windowStart would add a constant minute to every reading and bury the signal
            // this is meant to show: how far behind the exchange the derived output runs.
            metrics.endToEndLatency(Duration.between(candle.getWindowEnd(), now));
        }
        return KeyValue.pair(window.key(), candle);
    }
}
