#!/bin/sh

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic "${INGEST_TOPIC:-datat-ingest}" \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic "${INSERTER_METRICS_TOPIC:-inserter-metrics}" \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
