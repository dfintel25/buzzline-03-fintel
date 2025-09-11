"""
json_consumer_fintel.py

Consume json messages from a Kafka topic and process them.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}
"""


#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold author counts
#####################################

author_counts: defaultdict[str, int] = defaultdict(int)

#####################################
# Extra Data Store for Bob alerts
#####################################

bob_lie_count: int = 0


def check_bob_lie(message_dict: dict) -> None:
    """
    Detect if Bob sends the specific 'lie' message.
    Increment counter if so.
    """
    global bob_lie_count
    author = message_dict.get("author", "")
    msg_text = message_dict.get("message", "")

    if author == "Bob" and msg_text == "Data Engineering is my passion":
        bob_lie_count += 1
        logger.warning(
            f"⚠️ ALERT: Bob told lie #{bob_lie_count}! Message: {msg_text}"
        )
# Append to CSV
        with open("data/bob_lies.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([bob_lie_count, msg_text])

#####################################
# Function to process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")

        from typing import Any
        message_dict: dict[str, Any] = json.loads(message)

        logger.info(f"Processed JSON message: {message_dict}")

        author = message_dict.get("author", "unknown")
        logger.info(f"Message received from author: {author}")

        author_counts[author] += 1
        logger.info(f"Updated author counts: {dict(author_counts)}")

        # 🔔 Check if Bob is lying
        check_bob_lie(message_dict)

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()

    logger.info(f"Kafka consumer for topic '{topic}' closed.")
    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")
    

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()