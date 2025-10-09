from stream.FileStream import InputStream
from kafka import KafkaConsumer
import threading

class KafkaStream(InputStream):
    def __init__(self, topic, bootstrap_servers):
        super().__init__()
        kafka_created = False
        while not kafka_created:
            try:
                self.consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='my-group')
                kafka_created = True
            except Exception as e:
                print(f"Kafka connection failed: {e}. Retrying...", flush=True)

        
        # Start consuming in a separate thread
        self.consuming = True
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
    
    def _consume_messages(self):
        """Consume messages from Kafka and put them into the internal stream queue."""
        try:
            for message in self.consumer:
                if not self.consuming:
                    break
                # print(f"Received message: {message.value}", flush=True)
                self._stream.put(message.value.decode('utf-8') if isinstance(message.value, bytes) else message.value)
        except Exception as e:
            print(f"Error consuming messages: {e}", flush=True)
        finally:
            self._stream.put(None)  # Signal end of stream
    
    def close(self):
        """Close the Kafka consumer and stop consuming."""
        self.consuming = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        super().close()