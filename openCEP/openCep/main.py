from stream.KafkaStream import KafkaStream
import bike_query
from CEP import CEP
from stream.FileStream import FileOutputStream
from stream.StdOutStream import StdOutStream 
from plugin.cityBike.cityBike import CitiBikeCSVFormatter

from os import getenv  


print("Starting OpenCEP...",flush=True)
if __name__ == "__main__":
    log_level = getenv("LOG_LEVEL", "INFO")

    topic = getenv("INGEST_TOPIC", "ingest-topic")
    bootstrap_servers = getenv("KAFKA_SERVER", "localhost:9092")

    in_stream = KafkaStream(topic, bootstrap_servers)
    query = bike_query.hotPaths
    # out_stream = FileOutputStream("test/Matches", "output.txt")

    out_stream = StdOutStream()

    cep = CEP([query])
    cep.run(in_stream, out_stream, CitiBikeCSVFormatter())