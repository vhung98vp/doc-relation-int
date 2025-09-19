import uuid
from config import ES, ES_RL_PROPERTY
    

def build_relation_message(val_a, val_b, type_a=None, type_b=None):
    if type_a and type_b:
        return {
            ES_RL_PROPERTY['relation_type']: ES['relation_type'],
            ES_RL_PROPERTY['relation_id']: str(uuid.uuid4()),
            ES_RL_PROPERTY['from_entity']: val_a,
            ES_RL_PROPERTY['to_entity']: val_b,
            ES_RL_PROPERTY['from_entity_type']: type_a['entity'],
            ES_RL_PROPERTY['to_entity_type']: type_b['entity'],
            ES_RL_PROPERTY['from_source']: type_a['source'],
            ES_RL_PROPERTY['to_source']: type_b['source'],
            ES_RL_PROPERTY['from_datasource']: type_a['data_source'],
            ES_RL_PROPERTY['to_datasource']: type_b['data_source'],
            ES_RL_PROPERTY['create_user']: ES['create_user']
        }
    else:
        return {
            ES_RL_PROPERTY['relation_type']: ES['relation_type'],
            ES_RL_PROPERTY['relation_id']: str(uuid.uuid4()),
            ES_RL_PROPERTY['from_entity']: val_a['id'],
            ES_RL_PROPERTY['to_entity']: val_b['id'],
            ES_RL_PROPERTY['from_entity_type']: val_a['entity'],
            ES_RL_PROPERTY['to_entity_type']: val_b['entity'],
            ES_RL_PROPERTY['from_source']: val_a['source'],
            ES_RL_PROPERTY['to_source']: val_b['source'],
            ES_RL_PROPERTY['from_datasource']: val_a['data_source'],
            ES_RL_PROPERTY['to_datasource']: val_b['data_source'],
            ES_RL_PROPERTY['create_user']: ES['create_user']
        }
