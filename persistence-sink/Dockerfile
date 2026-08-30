# Build context is the repository root: this is a multi-module Maven build, and the
# sink cannot be built without its parent POM and the two library modules.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Copy only the POMs first so the dependency download is cached independently of the
# source. Touching a Java file then costs a recompile rather than a re-resolve.
COPY pom.xml .
COPY market-common/pom.xml market-common/
COPY market-schemas/pom.xml market-schemas/
COPY persistence-sink/pom.xml persistence-sink/
# The parent lists every module, so all of them must exist for the reactor to load — even
# though -pl builds only this one. Their sources are never copied.
COPY kraken-ingestor/pom.xml kraken-ingestor/
COPY market-processor/pom.xml market-processor/
COPY market-api/pom.xml market-api/
RUN mvn -B -q -pl persistence-sink -am dependency:go-offline -DskipTests || true

COPY market-common/src market-common/src
COPY market-schemas/src market-schemas/src
COPY persistence-sink/src persistence-sink/src
# Unit tests run in CI, not here: a slow machine must not be able to block an image build,
# and the image is built from a commit whose tests already passed.
RUN mvn -B -q -pl persistence-sink -am package -DskipTests


FROM eclipse-temurin:21-jre-alpine AS runtime

# No libstdc++ and no state volume, unlike the processor: the sink holds nothing between
# batches. Its durability lives in Kafka's committed offsets, which is the whole point of
# committing them only after ClickHouse acks.

# Non-root, and no shell account: the sink holds a Kafka connection and a ClickHouse
# credential, so a container escape should land somewhere useless (design doc 22).
RUN addgroup -S market && adduser -S -G market -H -s /sbin/nologin market

WORKDIR /app
COPY --from=build /build/persistence-sink/target/persistence-sink.jar /app/persistence-sink.jar

USER market
EXPOSE 9103

# Container memory, not the host's: without this the JVM sizes its heap from the host and
# gets OOM-killed under a container limit rather than throwing OutOfMemoryError. The
# percentage is the ingestor's rather than the processor's because this service allocates
# nothing off-heap — a batch of rows is all Java heap.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/persistence-sink.jar"]
