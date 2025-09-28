FROM sbtscala/scala-sbt:eclipse-temurin-24.0.1_9_1.11.6_3.7.3 AS builder
WORKDIR /build

COPY build.sbt ./
COPY project ./project

RUN sbt update

COPY . .
RUN sbt assembly


FROM gcr.io/distroless/java21-debian12 AS runner
WORKDIR /app

COPY --from=builder /build/target/scala-3.7.3/metrics-collector-assembly-1.0.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]