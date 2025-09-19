from kafka import start_kafka_consumer
from config import get_logger
logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting Kafka consumer and worker...")
    start_kafka_consumer()