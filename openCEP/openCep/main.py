from stream.KafkaStream import KafkaStream
import bike_query
from CEP import CEP
from stream.FileStream import FileOutputStream
from stream.StdOutStream import StdOutStream 
from plugin.cityBike.cityBike import CitiBikeCSVFormatter

import bike_query_test

from os import getenv  

import sys    
# import logging

print("Starting OpenCEP...",flush=True)
if __name__ == "__main__":
    log_level = getenv("LOG_LEVEL", "INFO")

    # logger = logging.getLogger("Main")
    # logger.setLevel(log_level)

    # logger.info("Starting OpenCEP with Kafka input stream...")

    topic = getenv("INGEST_TOPIC", "ingest-topic")
    bootstrap_servers = getenv("KAFKA_SERVER", "localhost:9092")

    # logger.info(f"Connecting to Kafka topic '{topic}' on server '{bootstrap_servers}'")

    # in_logger = logger.getChild("InputStream")
    # in_stream = KafkaStream(topic, bootstrap_servers, in_logger)
    in_stream = KafkaStream(topic, bootstrap_servers)
    query = bike_query.hotPathsPattern
    # query = bike_query_test.matchAnyTripPattern
    out_stream = FileOutputStream("test/Matches", "output.txt")

    # out_logger = logger.getChild("OutputStream")
    out_stream = StdOutStream()

    cep = CEP([query])
    cep.run(in_stream, out_stream, CitiBikeCSVFormatter())