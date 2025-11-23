import json
import logging
from confluent_kafka import Producer
import socket

logger = logging.getLogger("kafka-producer")


class KafkaProducer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'kafka:9092',
            'client.id': socket.gethostname(),
            'message.timeout.ms': 5000,
            'retries': 3
        }
        self.producer = Producer(self.conf)
        logger.info("Kafka producer initialized")

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def send_message(self, topic: str, message: dict):
        """Send message to Kafka topic"""
        try:
            # Trigger any available delivery report callbacks from previous produce() calls
            self.producer.poll(0)

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            self.producer.produce(
                topic=topic,
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer.flush()

            # In a real implementation, you might want to return actual partition and offset
            # For this MVP, we'll return mock values
            return {"partition": 0, "offset": 0}

        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
            raise e

    def __del__(self):
        """Cleanup producer on destruction"""
        if hasattr(self, 'producer'):
            self.producer.flush()