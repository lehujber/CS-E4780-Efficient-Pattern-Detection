from CEP import CEP
from stream.InsertStream import InsertStream
from stream.FileStream import FileOutputStream
from stream.KafkaOutStream import KafkaOutputStream
from plugin.cityBike.cityBike import CitiBikeCSVFormatter
from input_shedding import LoadSheddingPolicy as ls

import threading    

from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_SERVER, 
    INGEST_TOPIC, 
    MATCHES_TOPIC,
    QUERY_TYPE
)

# Import the appropriate query based on QUERY_TYPE
if QUERY_TYPE == "kleene":
    from kleene_query import kleenePattern as pattern, eval_params, parallel_params
    print("Using Kleene closure query", flush=True)
else:
    from bike_query import threeStationPattern as pattern, eval_params, parallel_params
    print("Using three station query", flush=True)

ls.load_probabilities()
ls.enable(THRESHOLD_MS)


def insert_messages(stream: InsertStream, consumer: KafkaConsumer):
    while True:
        try:
            for message in consumer:
                msg_value = message.value.decode('utf-8').strip()
                if msg_value == "EOS":
                    print("Received EOS message, closing stream.", flush=True)
                    stream.close()
                    return
                stream.add_item(msg_value)

        except Exception as e:
            print(f"Error consuming messages: {e}", flush=True)
        finally:
            stream.close()

def insert_file(stream: InsertStream, fname: str):
    print(f"Inserting events from file {fname}...", flush=True)
    with open(fname, "r") as f:
        for line in f:
            stream.add_item(line)
    stream.close()

def run_cep(cep: CEP, events: InsertStream, output: FileOutputStream, formatter: CitiBikeCSVFormatter):
    print("Running CEP engine...", flush=True)
    cep.run(events, output, formatter)

def main():
    cep = CEP([pattern], eval_params, parallel_params)

    events = InsertStream()

    kafka_consumer = KafkaConsumer(
        INGEST_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
    )

    # insert_task = loop.create_task(insert_file(events, "test/EventFiles/201801-citibike-tripdata.csv"))

    insert_thread = threading.Thread(target=insert_messages, args=(events, kafka_consumer))

    output_stream = KafkaOutputStream(
        KafkaProducer(bootstrap_servers=[KAFKA_SERVER]),
        MATCHES_TOPIC
    )

    cep_thread = threading.Thread(target=run_cep, args=(cep, events, output_stream, CitiBikeCSVFormatter()))

    insert_thread.start()
    cep_thread.start()
    insert_thread.join()
    print("Insert thread finished, waiting for CEP to complete...", flush=True)
    cep_thread.join()
    print("CEP processing finished.", flush=True)




if __name__ == "__main__":
    main()

    print("Done. Check test/Matches/output.txt for matches")
    while True:
        pass
