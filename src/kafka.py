import json
import time
import uuid
from itertools import combinations
from confluent_kafka import Consumer, Producer
from config import get_logger, KAFKA, KAFKA_CONSUMER_CONFIG, KAFKA_PRODUCER_CONFIG, ES_COMPANY, ES_PERSON, ES_DOC
from pattern import classify_by_type
from es import query_entity
from utils import build_relation_message, unique_dict_list, merge_dicts
logger = get_logger(__name__)

# Kafka setup
producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])


def process_vals_by_type(vals_by_type, type, doc_id):
    vals = {k: v for k, v in vals_by_type.items() if k in type['search_fields']}
    if vals:
        query_result = query_entity(vals, type['index'])
        if query_result:
            val_to_recs, found_ids = query_result
            for item in found_ids:
                send_output_to_kafka(build_relation_message(item, doc_id, type, ES_DOC))
            logger.info(f"Processed {len(found_ids)} existing {type['entity']} entities.")
            return val_to_recs
    return {}


def process_vals_table(vals_table, val_to_recs):
    count = 0
    for row in vals_table:
        row_records = []
        for record in row:
            row_records.extend(val_to_recs.get(record, []))

        row_records = unique_dict_list(row_records)
        if len(row_records) > 1:
            for val_a, val_b in combinations(row_records, 2):
                send_output_to_kafka(build_relation_message(val_a, val_b))
                count += 1
    logger.info(f"Processed {count} relations from table ids.")



def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        
        for key in ['_fs_internal_id', 'ids']:
            if key not in data:
                raise ValueError(f"Missing required field '{key}' in message")
        # Group by type (pattern)
        doc_id = data['_fs_internal_id']
        vals_search = data.get('ids', [])
        if vals_search:
            vals_by_type = classify_by_type(vals_search)
            logger.info(f"{len(vals_search)} search values mapped to {len(vals_by_type)} types.")

            # Process by type and vals
            recs_p = process_vals_by_type(vals_by_type, ES_PERSON, doc_id)
            recs_c = process_vals_by_type(vals_by_type, ES_COMPANY, doc_id)
            
            val_to_recs = merge_dicts(recs_p, recs_c)
            vals_table = data.get('table_ids', [])
            if vals_table:
                process_vals_table(vals_table, val_to_recs)
            else:
                logger.warning("No table_ids found in message.")

        logger.info(f"Message {msg_key} processed in time (s): {time.time()-start_time:4f}.")


    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka(msg_key, { 
            "error": str(e), 
            "message": msg 
        })
        raise e
    finally:
        logger.info(f"Processed message {msg_key} in {time.time() - start_time:.4f} seconds")


def start_kafka_consumer():
    processed_count = 0
    error_count = 0
    last_wait_time = 0
    try:
        while True:
            msg = consumer.poll(KAFKA['consumer_timeout'])
            if msg is None or msg.error():
                if msg is None:
                    cur_time = time.time()
                    if cur_time - last_wait_time > 60:
                        logger.info("Waiting for messages...")
                        last_wait_time = cur_time
                else:
                    logger.error(f"Message error: {msg.error()}")
                continue
            try:
                message = msg.value().decode("utf-8")
                message_key = msg.key().decode("utf-8") if msg.key() else None
                if not message_key:
                    logger.warning(f"Received message without key: {message}")
                process_message(message_key, message)
                processed_count += 1
            except Exception as e:
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        producer.flush()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")


def send_output_to_kafka(result):
    try:
        producer.produce(KAFKA['output_topic'], key=str(uuid.uuid4()), value=json.dumps(result, ensure_ascii=False))
        producer.poll(0)
    except Exception as e:
        logger.exception(f"Error sending result to output topic: {e}")


def log_error_to_kafka(msg_key, error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], key=msg_key, value=json.dumps(error_info, ensure_ascii=False))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")
