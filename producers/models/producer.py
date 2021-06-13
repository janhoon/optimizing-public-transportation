"""Producer base-class providing common utilities and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "client.id": "cta-data-transit"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            # Add schema registry only when creating Avro Producer.
            config={**self.broker_properties, "schema.registry.url": "http://localhost:8081"},
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # Code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        admin_client = AdminClient(conf=self.broker_properties)

        if admin_client.list_topics().topics.get(self.topic_name) is None:
            topics = [
                NewTopic(self.topic_name, 1, 1)
            ]

            admin_client.create_topics(topics)
            logging.info(f"Created new topic: {self.topic_name}")
        else:
            logging.info(f"Topic already exists: {self.topic_name}")

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # Cleanup code for the Producer here
        self.producer.flush()
        logger.info("Producer messages flushed")
