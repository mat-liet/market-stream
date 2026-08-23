# Build context is the repository root: this is a multi-module Maven build, and the
# ingestor cannot be built without its parent POM and the two library modules.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Copy only the POMs first so the dependency download is cached independently of the
# source. Touching a Java file then costs a recompile rather than a re-resolve.
COPY pom.xml .
COPY market-common/pom.xml market-common/
COPY market-schemas/pom.xml market-schemas/
COPY kraken-ingestor/pom.xml kraken-ingestor/
# The parent lists every module, so all of them must exist for the reactor to load — even
# though -pl builds only this one. Their sources are never copied.
COPY market-processor/pom.xml market-processor/
COPY persistence-sink/pom.xml persistence-sink/
COPY market-api/pom.xml market-api/
RUN mvn -B -q -pl kraken-ingestor -am dependency:go-offline -DskipTests || true

COPY market-common/src market-common/src
COPY market-schemas/src market-schemas/src
COPY kraken-ingestor/src kraken-ingestor/src
# Unit tests run in CI, not here: a failing exchange or a slow machine must not be able to
# block an image build, and the image is built from a commit whose tests already passed.
RUN mvn -B -q -pl kraken-ingestor -am package -DskipTests


FROM eclipse-temurin:21-jre-alpine AS runtime

# Non-root, and no shell account: the ingestor holds a Kafka connection and a Postgres
# credential, so a container escape should land somewhere useless (design doc 22).
RUN addgroup -S market && adduser -S -G market -H -s /sbin/nologin market

WORKDIR /app
COPY --from=build /build/kraken-ingestor/target/kraken-ingestor.jar /app/kraken-ingestor.jar

USER market
EXPOSE 9101

# Container memory, not the host's: without this the JVM sizes its heap from the host and
# gets OOM-killed under a container limit rather than throwing OutOfMemoryError.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/kraken-ingestor.jar"]
