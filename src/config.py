import os
import sys
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', 
                    handlers=[logging.StreamHandler(sys.stdout)]
                )

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger
logger = get_logger(__name__)

KAFKA = {
    'brokers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
    'consumer_group': os.environ.get('KAFKA_CONSUMER_GROUP', 'default'),
    'consumer_timeout': float(os.environ.get('KAFKA_CONSUMER_TIMEOUT', 1)),
    'auto_offset_reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
    'input_topic': os.environ.get('KAFKA_INPUT_TOPIC'),
    'output_topic': os.environ.get('KAFKA_OUTPUT_TOPIC'),
    'error_topic': os.environ.get('KAFKA_ERROR_TOPIC')
}

KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers'],
    'group.id': KAFKA['consumer_group'],
    'auto.offset.reset': KAFKA['auto_offset_reset']
}

KAFKA_PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers']
}

ES = {
    'url': os.environ.get('ES_URL'),
    'user': os.environ.get('ES_USER'),
    'password': os.environ.get('ES_PASSWORD'),
    'relation_type': os.environ.get('ES_RELATION_TYPE'),
    'create_user': int(os.environ.get('ES_CREATE_USER', 1)),
    'index_suffix': os.environ.get('ES_INDEX_SUFFIX', '_idx')
}

ES_PERSON = {
    'entity_type': os.environ.get('ES_PERSON_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_PERSON_ENTITY_SOURCE', 0)),
    'datasource': int(os.environ.get('ES_PERSON_ENTITY_DATASOURCE', 0)),
    'index': f"{os.environ.get('ES_PERSON_ENTITY_DATASOURCE', 0)}{ES['index_suffix']}"
}

ES_COMPANY = {
    'entity_type': os.environ.get('ES_COMPANY_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_COMPANY_ENTITY_SOURCE', 0)),
    'datasource': int(os.environ.get('ES_COMPANY_ENTITY_DATASOURCE', 0)),
    'index': f"{os.environ.get('ES_PERSON_ENTITY_DATASOURCE', 0)}{ES['index_suffix']}"
}

ES_DOC = {
    'entity_type': os.environ.get('ES_DOC_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_DOC_ENTITY_SOURCE', 0)),
    'datasource': int(os.environ.get('ES_DOC_ENTITY_DATASOURCE', 0)),
    'index': f"{os.environ.get('ES_DOC_ENTITY_DATASOURCE', 0)}{ES['index_suffix']}"
}

with open ("config.json", "r") as f:
    conf = json.load(f)
    ES_RL_PROPERTY = conf['relation_properties']
    ES_SEARCH_KEY = conf['search_keys']
    ES_PERSON['search_fields'] = conf['search_fields']['person']
    ES_COMPANY['search_fields'] = conf['search_fields']['company']


if not KAFKA['brokers']:
    logger.error("KAFKA_BOOTSTRAP_SERVER is not set")
    sys.exit(1)

if not ES['url']:
    logger.error("ES_URL is not set")
    sys.exit(1)