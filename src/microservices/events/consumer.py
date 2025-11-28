import json
import logging
from confluent_kafka import Consumer, KafkaError
import threading

logger = logging.getLogger("kafka-consumer")


class KafkaConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'events-service-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        self.consumer = Consumer(self.conf)
        self.topics = ['movie-events', 'user-events', 'payment-events']
        self.running = False
        logger.info("Kafka consumer initialized")

    def process_message(self, msg):
        """Process incoming Kafka message"""
        try:
            message_value = json.loads(msg.value().decode('utf-8'))
            logger.info(f"📩 Received message from topic '{msg.topic()}':")
            logger.info(f"   Event ID: {message_value.get('id')}")
            logger.info(f"   Event Type: {message_value.get('type')}")
            logger.info(f"   Timestamp: {message_value.get('timestamp')}")
            logger.info(f"   Payload: {json.dumps(message_value.get('payload'), indent=2)}")
            logger.info(f"   Partition: {msg.partition()}, Offset: {msg.offset()}")
            logger.info("=" * 50)

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        try:
            self.consumer.subscribe(self.topics)
            self.running = True
            logger.info(f"Subscribed to topics: {self.topics}")

            while self.running:
                msg = self.consumer.poll(1.0)  # Wait for 1 second

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f'Reached end of partition {msg.partition()}')
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                else:
                    self.process_message(msg)

        except Exception as e:
            logger.error(f"Error in consumer loop: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Close consumer connection"""
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        logger.info("Kafka consumer closed")