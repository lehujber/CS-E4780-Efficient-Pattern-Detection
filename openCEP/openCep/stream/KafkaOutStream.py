from .Stream import OutputStream
import os
from base.PatternMatch import PatternMatch
from kafka import KafkaProducer
import json

class KafkaOutputStream(OutputStream):
    """
    A stream sending its items to a Kafka topic.
    """
    def __init__(self, kafka_producer: KafkaProducer, topic: str):
        super().__init__()
        self._kafka_producer = kafka_producer
        self._topic = topic
        self.__base_path = base_path
        self.latencies = []


    def add_item(self, item: object):
        """
        Sends the item to the Kafka topic.
        """
        self._kafka_producer.send(self._topic, f"{item}".encode('utf-8'))
        # Calculate latency if the item is a PatternMatch
        if isinstance(item, PatternMatch):
            arrival_time = item.get_last_event_arrival_timestamp()
            if arrival_time is not None:
                emission_time = time.perf_counter()
                latency_ms = (emission_time - arrival_time) * 1000  # Convert to milliseconds
                self.latencies.append(latency_ms)

    def close(self):
        """
        Closes the Kafka producer.
        """
        super().close()
        self._kafka_producer.flush()
        self._kafka_producer.send(self._topic, "EOS".encode('utf-8'))
        self._kafka_producer.close()

                # Print statistics to terminal and write raw latencies to file
        if self.latencies:
            # Print statistics to terminal
            print("\n" + "="*50)
            print("LATENCY STATISTICS")
            print("="*50)
            print(f"Total matches: {len(self.latencies)}")
            print(f"Average latency: {sum(self.latencies) / len(self.latencies):.3f} ms")
            print(f"Min latency: {min(self.latencies):.3f} ms")
            print(f"Max latency: {max(self.latencies):.3f} ms")
            print("="*50 + "\n")
            
            # Write only raw latency values to file (one per line)
            latency_file_path = os.path.join(self.__base_path, "latencies.txt")
            with open(latency_file_path, 'w') as f:
                for latency in self.latencies:
                    f.write(f"{latency:.3f}\n")