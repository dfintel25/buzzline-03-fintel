"""
json_consumer_fintel.py

Consume JSON messages from a Kafka topic, detect Bob's lies, and log them.
"""

#####################################
# Import Modules
#####################################

import os
import json
from kafka import KafkaConsumer
from utils.utils_logger import logger
from utils.utils_consumer import get_kafka_broker_address

#####################################
# Global Variables
#####################################

bob_lie_count = 0  # Track Bob's lies

#####################################
# Helper Functions
#####################################

def check_bob_lie(message: dict):
    """
    Check if message is from Bob and matches the lie condition.
    """
    global bob_lie_count
    if (
        message.get("author") == "Bob"
        and message.get("message") == "Data Engineering is my passion"
    ):
        bob_lie_count += 1
        logger.warning(f"🚨 Bob lied again! Total lies so far: {bob_lie_count}")

def process_message(message: dict):
    """
    Process each Kafka message.
    """
    try:
        logger.info(f"Received message: {message}")
        check_bob_lie(message)
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Consumer Creation
#####################################

def create_consumer(topic: str, group_id: str):
    """
    Create a KafkaConsumer safely for Windows.
    """
    kafka_broker = get_kafka_broker_address()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,  # allows graceful shutdown on Windows
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(f"Kafka consumer created for topic '{topic}' with group '{group_id}'.")
    logger.info(f"Assigned partitions: {consumer.assignment()}")
    return consumer

#####################################
# Main Consumer Loop
#####################################

def main():
    topic_name = os.getenv("BUZZ_TOPIC", "test_topic")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "fintel_group")

    consumer = create_consumer(topic_name, group_id)

    logger.info(f"Starting JSON consumer on topic '{topic_name}'...")

    try:
        while True:
            try:
                for msg in consumer:
                    process_message(msg.value)
                # loop exits after consumer_timeout_ms if no messages
            except Exception as e:
                logger.error(f"Error consuming messages: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down...")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
