"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # Configure the broker properties
        self.broker_properties = {
            "kafka_broker_url": "PLAINTEXT:localhost:9092",
            "schema_registry_url": "http://localhost:8081",
        }

        # Create the Consumer, using the appropriate type.
        consumer_config = {
            'bootstrap.servers': self.broker_properties['kafka_broker_url'],
            'group.id': "my-consumer-group", 
            'auto.offset.reset': "earliest" if self.offset_earliest else "latest"
        }
        if is_avro is True:
            consumer_config["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(consumer_config)
        else:
            self.consumer = Consumer(consumer_config)

        # subscribe to the topics
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest`, set the partition offset to the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
                logger.info(f"Setting partition {partition.partition} to start from the earliest offset")
        
        logger.info("Partitions assigned for topic: %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # Poll Kafka for messages. Make sure to handle any errors or exceptions.
        message = self.consumer.poll(timeout=self.consume_timeout)
        if message is None:
            return 0
        elif message.error() is not None:
            logger.error(f"Error while consuming messages.error() - {message.error()}")
            return 0
        else:
            try:
                self.message_handler(message)
                logger.info(f"Message consumed from {message.topic()} | key ({message.key()}), value ({message.value()})")
                return 1
            
            except SerializerError as e:
                logger.error(f"Message deserialization failed for {message.key()}: {e}")
                return 0
    
            except Exception as e:
                logger.error(f"Unexpected message consumption error for {message.key()}: {e}")
                return 0
            


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info("Close and clean up consumer.")
        
