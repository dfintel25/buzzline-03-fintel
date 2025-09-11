"""
json_consumer_fintel.py

Consume JSON messages from a Kafka topic, detect Bob's lies, and log them.
"""

#####################################
# Import Modules
#####################################

import json
import sys
import os

# Import functions from local utils
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer


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
        check_bob_lie(message)  # 👈 detect Bob's lies
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Consumer Loop
#####################################

def main():
    topic_name = os.getenv("KAFKA_TOPIC", "test_topic")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "fintel_group")

    consumer = create_kafka_consumer(
        topic_provided=topic_name,
        group_id_provided=group_id,
        value_deserializer_provided=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(f"Starting JSON consumer on topic '{topic_name}'...")

    try:
        for msg in consumer:
            process_message(msg.value)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Shutting down...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
