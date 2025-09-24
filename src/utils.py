import uuid
from config import ES, ES_RL_PROPERTY


def merge_dicts(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key].extend(value)
        else:
            dict1[key] = value
    return dict1

def unique_dict_list(dict_list):
    seen = set()
    unique_list = []
    for d in dict_list:
        if d["id"] not in seen:
            seen.add(d["id"])
            unique_list.append(d)
    return unique_list


def build_relation_message(val_a, val_b, type_b=None):
    if type_b:
        return {
            ES_RL_PROPERTY['relation_type']: ES['relation_type'],
            ES_RL_PROPERTY['relation_id']: str(uuid.uuid4()),
            ES_RL_PROPERTY['from_entity']: val_a['id'],
            ES_RL_PROPERTY['to_entity']: val_b,
            ES_RL_PROPERTY['from_entity_type']: val_a['entity'],
            ES_RL_PROPERTY['to_entity_type']: type_b['entity'],
            ES_RL_PROPERTY['from_source']: val_a['source'],
            ES_RL_PROPERTY['to_source']: type_b['source'],
            ES_RL_PROPERTY['from_datasource']: val_a['data_source'],
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
