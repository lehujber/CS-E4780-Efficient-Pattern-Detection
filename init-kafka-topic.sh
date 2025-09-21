#!/bin/sh

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic "${TOPIC_NAME:-datat-source}" \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
