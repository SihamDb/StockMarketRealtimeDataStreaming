import json
import os
import time
from confluent_kafka import Producer
from apis.market_data import generate_market_data
from configs.config import setup

# Set up the environment variables
setup()

# Kafka producer configuration
config = {
    "bootstrap.servers": os.getenv("KAFKA_BROKER"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "client.id": "transaction-producer",
    "acks": "all",
    "retries": 5,
    "batch.size": 16384,
    "linger.ms": 5,
    "compression.type": "gzip",
}

# Initialize the Kafka producer
producer = Producer(config)

# Define the topic to send data to
topic = os.getenv("KAFKA_TOPIC")


# Callback to handle delivery reports (called once for each message)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] at Offset: {msg.offset()}"
        )


def publish_messages(max_messages, ticker_name, message_interval=5):
    try:
        print(f"Publishing time series data to Kafka topic '{topic}'...")
        counter = 0
        while True and counter <= max_messages:
            key = ticker_name
            value = json.dumps(generate_market_data(counter, ticker_name))
            producer.produce(
                topic=topic, key=key, value=value, callback=delivery_report
            )
            print(f"Published: {value}")
            counter += 1

            # Poll to trigger the delivery report callback
            producer.poll(0)

            time.sleep(message_interval)

    except KeyboardInterrupt:
        print("Stopped publishing.")


# Run the producer function
if __name__ == "__main__":
    publish_messages(1000, "IBM")
