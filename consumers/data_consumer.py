import json
import os

import pandas as pd
from confluent_kafka import Consumer, KafkaException

from configs.config import setup
from visualisations.ploty_visualiser import PlotyVisualiserV2


class DataConsumer:
    def __init__(
        self,
        bootstrap_servers,
        sasl_username,
        sasl_password,
        topic,
        client_id,
        group_id="example_consumer_group",
    ):
        """Initialize the KafkaConsumer instance with the required configurations."""
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
            "client.id": client_id,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.topic = topic
        self.consumer = None
        self.visualizer = PlotyVisualiserV2()

    def start_consumer(self):
        """Initialize the consumer and subscribe to the topic."""
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic])
        print(f"Subscribed to topic: {self.topic}")
        self.visualizer.run_server()

    def consume_messages(self, timeout=1.0):
        """Start consuming messages from the Kafka topic."""
        if not self.consumer:
            raise Exception("Consumer not initialized. Call 'start_consumer()' first.")

        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Process the message
                print(f"Received message: key={msg.key()}, value={msg.value()}")

                message_value = json.loads(msg.value().decode("utf-8"))

                # Access the dictionary's keys safely
                timestamp = pd.to_datetime(message_value["window_start"])
                price = message_value["average_price"]

                print(f"Decoded message: timestamp={timestamp}, price={price}")

                self.visualizer.append_data(timestamp, price)

                # Commit the offset
                self.consumer.commit(msg)

        except KafkaException as e:
            print(f"Kafka error occurred: {e}")

        except KeyboardInterrupt:
            print("Consumer interrupted by user")

        finally:
            self.close_consumer()

    def close_consumer(self):
        """Close the consumer to release resources."""
        if self.consumer:
            self.consumer.close()
            print("Consumer closed")


# Example usage (with environment variables for sensitive information)
if __name__ == "__main__":
    # setup the environment variables
    setup()

    # Initialize the data consumer
    kafka_consumer = DataConsumer(
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        sasl_username=os.getenv("KAFKA_API_KEY"),
        sasl_password=os.getenv("KAFKA_API_SECRET"),
        topic=os.getenv("KAFKA_TOPIC_PROCESSED"),
        client_id="example_consumer7",
        group_id="example_consumer_group7",
    )

    kafka_consumer.start_consumer()
    kafka_consumer.consume_messages()
