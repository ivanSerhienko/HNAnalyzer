# --- Build stage ---
FROM sbtscala/scala-sbt:eclipse-temurin-21.0.11_10_1.12.15_3.8.4 AS build
WORKDIR /app

# Copy just the build definition first so dependency resolution is cached
# as its own layer, independent of source code changes.
COPY project project
COPY build.sbt .
RUN sbt update

COPY src src
RUN sbt stage

# --- Runtime stage ---
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app
COPY --from=build /app/target/universal/stage /app
ENTRYPOINT ["/app/bin/app"]
