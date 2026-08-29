# Build context is the repository root: this is a multi-module Maven build, and the
# processor cannot be built without its parent POM and the two library modules.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Copy only the POMs first so the dependency download is cached independently of the
# source. Touching a Java file then costs a recompile rather than a re-resolve.
COPY pom.xml .
COPY market-common/pom.xml market-common/
COPY market-schemas/pom.xml market-schemas/
COPY market-processor/pom.xml market-processor/
# The parent lists every module, so all of them must exist for the reactor to load — even
# though -pl builds only this one. Their sources are never copied.
COPY kraken-ingestor/pom.xml kraken-ingestor/
COPY persistence-sink/pom.xml persistence-sink/
COPY market-api/pom.xml market-api/
RUN mvn -B -q -pl market-processor -am dependency:go-offline -DskipTests || true

COPY market-common/src market-common/src
COPY market-schemas/src market-schemas/src
COPY market-processor/src market-processor/src
# Unit tests run in CI, not here: a slow machine must not be able to block an image build,
# and the image is built from a commit whose tests already passed.
RUN mvn -B -q -pl market-processor -am package -DskipTests


FROM eclipse-temurin:21-jre-alpine AS runtime

# RocksDB backs every Streams state store, and its bundled native library links against
# libstdc++, which the Alpine JRE image does not ship. Without this the JNI load fails with
# UnsatisfiedLinkError, every stateful task dies on start, and the process keeps running —
# normalisation carries on while no candle is ever produced. The ingestor's image needs
# none of this because it holds no state.
RUN apk add --no-cache libstdc++

# Non-root, and no shell account: the processor holds a Kafka connection and a Postgres
# credential, so a container escape should land somewhere useless (design doc 22).
RUN addgroup -S market && adduser -S -G market -H -s /sbin/nologin market

# Created and chowned before the volume is mounted so Docker gives a fresh named volume
# this ownership. Without it the volume arrives root-owned and RocksDB cannot open a store.
RUN mkdir -p /var/lib/market-processor && chown market:market /var/lib/market-processor

WORKDIR /app
COPY --from=build /build/market-processor/target/market-processor.jar /app/market-processor.jar

USER market
EXPOSE 9102

# Container memory, not the host's: without this the JVM sizes its heap from the host and
# gets OOM-killed under a container limit rather than throwing OutOfMemoryError. RocksDB
# allocates off-heap on top of this, which is why the percentage is lower than the
# ingestor's — the processor's memory is not all Java heap.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/market-processor.jar"]
