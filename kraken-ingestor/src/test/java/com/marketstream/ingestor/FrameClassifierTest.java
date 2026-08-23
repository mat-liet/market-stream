package com.marketstream.ingestor;

import static org.assertj.core.api.Assertions.assertThat;

import com.marketstream.ingestor.FrameClassifier.Frame;
import com.marketstream.ingestor.FrameClassifier.FrameType;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Drives the payload shapes documented for Kraken WS v2 verbatim.
 *
 * <p>These are copies of the documented examples rather than invented samples, because the
 * classifier's whole value is that it agrees with what Kraken actually sends. A test built
 * from the same assumptions as the code would agree with the code and not with the exchange.
 */
class FrameClassifierTest {

    private final FrameClassifier classifier = new FrameClassifier();

    private Frame classify(String json) {
        return classifier.classify(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void classifiesTradeUpdateAndExtractsSymbol() {
        Frame frame = classify("""
                {"channel":"trade","type":"update","data":[
                  {"symbol":"BTC/USD","side":"sell","price":60000.1,"qty":0.001,
                   "ord_type":"market","trade_id":70154386,"timestamp":"2026-08-23T12:00:00.123456Z"}]}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.TRADE_DATA);
        assertThat(frame.channel()).isEqualTo("trade");
        assertThat(frame.symbol()).isEqualTo("BTC/USD");
        assertThat(frame.type().isPublishable()).isTrue();
    }

    @Test
    void classifiesTradeSnapshotTheSameWayAsAnUpdate() {
        // The ingestor must not care about the distinction: both are trade data, and
        // deciding what a snapshot means is normalisation's job, not ingestion's.
        Frame frame = classify("""
                {"channel":"trade","type":"snapshot","data":[
                  {"symbol":"ETH/USD","side":"buy","price":2600.5,"qty":1.2,
                   "ord_type":"limit","trade_id":1,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.TRADE_DATA);
        assertThat(frame.symbol()).isEqualTo("ETH/USD");
    }

    @Test
    void classifiesBookUpdate() {
        Frame frame = classify("""
                {"channel":"book","type":"update","data":[
                  {"symbol":"BTC/USD","bids":[{"price":60000.0,"qty":1.0}],"asks":[],
                   "checksum":2439117997,"timestamp":"2026-08-23T12:00:00.000000Z"}]}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.BOOK_DATA);
        assertThat(frame.channel()).isEqualTo("book");
        assertThat(frame.symbol()).isEqualTo("BTC/USD");
    }

    @Test
    void classifiesHeartbeat() {
        Frame frame = classify("{\"channel\":\"heartbeat\"}");

        assertThat(frame.type()).isEqualTo(FrameType.HEARTBEAT);
        assertThat(frame.symbol()).isNull();
        assertThat(frame.type().isPublishable()).isFalse();
    }

    @Test
    void readsConnectionIdFromStatusAsTextBecauseItOverflowsALong() {
        // The documented example. Parsing it as a long would silently yield a different id,
        // and the id is what ties a frame to the connection that delivered it.
        Frame frame = classify("""
                {"channel":"status","type":"update","data":[
                  {"api_version":"v2","connection_id":13834774380200032777,
                   "system":"online","version":"2.0.9"}]}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.STATUS);
        assertThat(frame.connectionId()).isEqualTo("13834774380200032777");
        assertThat(new BigInteger(frame.connectionId()))
                .isGreaterThan(BigInteger.valueOf(Long.MAX_VALUE));
    }

    @Test
    void classifiesSubscribeAcknowledgement() {
        Frame frame = classify("""
                {"method":"subscribe","req_id":1,"result":{"channel":"trade","snapshot":false,
                 "symbol":"BTC/USD"},"success":true,"time_in":"2026-08-23T12:00:00.0Z",
                 "time_out":"2026-08-23T12:00:00.1Z"}
                """);

        // An ack carries result.channel, so it must be recognised before the channel lookup
        // or it would be mistaken for trade data and published.
        assertThat(frame.type()).isEqualTo(FrameType.SUBSCRIBE_ACK);
        assertThat(frame.type().isPublishable()).isFalse();
    }

    @Test
    void classifiesFailedSubscribeAsAnAcknowledgementToo() {
        Frame frame = classify("""
                {"error":"Subscription Not Found","method":"unsubscribe","req_id":2,"success":false}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.SUBSCRIBE_ACK);
    }

    @Test
    void reportsMalformedJsonSeparatelyFromUnknownChannels() {
        // A truncated frame is what a partial-message assembly bug looks like, so it must
        // not be filed under "Kraken sent us something new".
        assertThat(classify("{\"channel\":\"trade\",\"data\":[").type()).isEqualTo(FrameType.MALFORMED);
        assertThat(classify("not json at all").type()).isEqualTo(FrameType.MALFORMED);
        assertThat(classify("").type()).isEqualTo(FrameType.MALFORMED);
    }

    @Test
    void treatsAJsonArrayAsMalformedBecauseV2FramesAreObjects() {
        // This is the WS v1 shape. Receiving one means we are pointed at the wrong endpoint.
        assertThat(classify("[0,[[\"5541.2\",\"0.15\",\"1534614057.3\"]],\"trade\"]").type())
                .isEqualTo(FrameType.MALFORMED);
    }

    @Test
    void classifiesAnUnrecognisedChannelAsUnknownWithoutLosingItsSymbol() {
        Frame frame = classify("""
                {"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":60000.0}]}
                """);

        assertThat(frame.type()).isEqualTo(FrameType.UNKNOWN);
        assertThat(frame.channel()).isEqualTo("ticker");
        assertThat(frame.symbol()).isEqualTo("BTC/USD");
    }

    @Test
    void handlesDataFramesWithNoUsableSymbol() {
        assertThat(classify("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[]}").symbol()).isNull();
        assertThat(classify("{\"channel\":\"trade\",\"type\":\"update\"}").symbol()).isNull();
        assertThat(classify("{\"channel\":\"trade\",\"data\":[{\"price\":1.0}]}").symbol()).isNull();
    }
}
