from .Stream import OutputStream
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

    def add_item(self, item: object):
        """
        Sends the item to the Kafka topic.
        """
        self._kafka_producer.send(self._topic, f"{item}".encode('utf-8'))

    def close(self):
        """
        Closes the Kafka producer.
        """
        super().close()
        self._kafka_producer.flush()
        self._kafka_producer.send(self._topic, "EOS".encode('utf-8'))
        self._kafka_producer.close()