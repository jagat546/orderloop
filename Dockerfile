# syntax=docker/dockerfile:1
# Service images for the demo (roadmap M1.16a), built from source so the demo needs only Docker.
# One build stage compiles every service; each image takes only its own executable jar:
#   docker build --target service --build-arg MODULE=payment-service .
# The root compose.yaml builds all four. Not a production image; see docs/DEPLOYMENT.md.

# Maven 3.9.12 on Temurin 25, the versions the project builds with (ADR 0001).
FROM maven:3.9.12-eclipse-temurin-25 AS build
WORKDIR /src
COPY . .
# Tests run in CI and with mvn verify; the image only packages. The cache keeps downloads
# between builds.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests \
      -pl order-orchestrator,inventory-service,payment-service,fulfilment-service -am package

# The JRE of the same Temurin update the build used.
FROM eclipse-temurin:25.0.5_8-jre AS service
ARG MODULE
# curl for the Compose healthcheck; the JRE image has no HTTP client.
RUN test -n "$MODULE" \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home orderloop
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-*-exec.jar app.jar
USER orderloop
# Sized by the container's memory limit, with the small serial collector; a JVM that runs out
# of memory exits so Compose restarts it instead of limping on. UTC whatever the host's zone.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -XX:+UseSerialGC -XX:+ExitOnOutOfMemoryError -Duser.timezone=UTC"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
