# market-stream

A real-time market-data processing platform built on **Java 21 · Kafka · Kafka Streams · Avro · ClickHouse**.

It ingests Kraken's public WebSocket v2 feed, publishes every frame to Kafka as an immutable raw event, normalises it into an exchange-agnostic canonical model, and derives real-time market metrics — trade candles, VWAP, rolling statistics, order-book spread/depth/imbalance — with Kafka Streams. Derived data is copied to a columnar time-series store for historical querying, and a read-only API exposes current state and history without ever sitting in the ingestion path.

> **Status: design complete, implementation starting.** The full technical design is in [`market-data-platform-design.md`](market-data-platform-design.md).

## Design principles

1. **Kafka is the source of truth.** Every downstream artifact — candles, metrics, alerts, the analytical database — is a deterministic function of the raw event log, which makes replay and reprocessing first-class rather than bolted on.
2. **Correctness is explicit and observable.** Order books are governed by a formal state machine; the processor refuses to publish trusted metrics whenever the local book cannot be verified against Kraken's CRC32 checksum. Late, duplicate, and out-of-order events are handled deliberately.
3. **Four deployables, not a microservice swarm.** Ingestor, Stream Processor, Persistence Sink, API — each with one job and a clean Kafka boundary.
4. **Simple first, but not a dead end.** Every early simplification (single broker, RF=1, two markets, one processor instance) has a documented evolution path that does not require a rewrite.

The linchpin decision is **keying every event by `(exchange, instrument)`**, which gives per-order-book ordering on a single partition, makes each book independently parallelisable, and generalises to multi-exchange unchanged.

## Architecture

```
Kraken WS v2 → kraken-ingestor → raw.kraken.*
                                      ↓
                            market-processor (Kafka Streams, EOS v2)
                                      ↓
              normalized.* → derived.* / alerts / state.book.current
                                      ↓
                              persistence-sink → ClickHouse
                                      ↓
                                  market-api (REST + SSE)
```

| Service | Responsibility |
|---|---|
| `kraken-ingestor` | Maintain healthy Kraken WS connections; publish raw frames verbatim to Kafka. Deliberately dumb. |
| `market-processor` | Normalise, reconstruct and verify order books, compute event-time windowed metrics, raise alerts. The only stateful, correctness-critical component. |
| `persistence-sink` | Copy derived topics into ClickHouse idempotently and in batches, applying backpressure via Kafka lag rather than dropping data. |
| `market-api` | Serve current + historical data, read-only, off the critical path. |

## Scope of v1

BTC/USD and ETH/USD, single-node local deployment via Docker Compose, with an architecture credible for multi-market and multi-exchange growth.

**Non-goals:** no trading or private Kraken channels, no second exchange implementation (only the abstractions for one), no Kubernetes or multi-region, no sub-millisecond HFT latency target.

## Roadmap

| Phase | Scope |
|---|---|
| **1** | Trade pipeline walking skeleton, end-to-end: WS → raw → EOS processor → 1m OHLCV/VWAP → ClickHouse → REST |
| **2** | Stateful trade processing: 1s/10s/1m windows, late-event handling, dedupe, rolling metrics, alerts, replay harness |
| **3** | Order books: snapshot + update reconstruction, CRC32 validation, the state machine, spread/depth/imbalance |
| **4** | Reliability and scale: failure tests, backpressure validation, rebalancing, standby replicas, load testing |
| **5** | Multi-exchange readiness: the `ExchangeAdapter` abstraction and symbol mapping — not a second exchange |

See [§26 of the design](market-data-platform-design.md#26-phased-implementation-plan) for deliverables, risks, and completion criteria per phase.

## Getting started

Requires Docker and a **JDK 21** (the build enforces the version rather than silently
producing bytecode from whichever JDK Maven happened to resolve).

```bash
make up            # Kafka, Schema Registry, ClickHouse, Postgres, Prometheus, Grafana
make build         # compile and test every module
make up-services   # additionally build and run the containerised services
make lag           # per-consumer-group lag
make reset         # wipe all volumes for a clean slate
```

`make up` is deliberately infrastructure-only, so the inner loop is to run a service from
the IDE against the stack. `make up-services` brings up the containerised copy for testing
the deployed shape.

Integration tests (`*IT`) run against the `make up` stack and **skip** when it is not
running, so `mvn verify` passes either way.
