import os

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
INGEST_TOPIC = os.getenv("INGEST_TOPIC", "data-ingest")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
WORKER_THREADS = int(os.getenv("WORKER_THREADS", 1))
MATCHES_TOPIC = os.getenv("MATCHES_TOPIC", "matches")