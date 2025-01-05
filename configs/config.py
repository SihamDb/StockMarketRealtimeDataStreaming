from configs.config_dev import kafka_conf
import os


def setup():
    # Set environment variables
    os.environ["KAFKA_BROKER"] = kafka_conf["KAFKA_BROKER"]
    os.environ["KAFKA_TOPIC"] = kafka_conf["KAFKA_TOPIC"]
    os.environ["KAFKA_TOPIC_PROCESSED"] = kafka_conf["KAFKA_TOPIC_PROCESSED"]
    os.environ["KAFKA_API_KEY"] = kafka_conf["KAFKA_API_KEY"]
    os.environ["KAFKA_API_SECRET"] = kafka_conf["KAFKA_API_SECRET"]
