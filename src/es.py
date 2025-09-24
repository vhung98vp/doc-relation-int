import requests
from config import ES, get_logger
from utils import unique_dict_list
logger = get_logger(__name__)

def query_entity(query_vals, type, size=10000):
    url = f"{ES['url']}/{type['index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "bool": {
                "should": [
                    {"terms": {f"properties.{key}": vals}} 
                        for key, vals in query_vals.items()
                ],
                "minimum_should_match": 1
            }
        },
        "_source": ["id", "entity", "source", "data_source", "properties"],
        "size": size
    }
    try:
        response = requests.get(url=url,
                                    headers=headers,
                                    auth=auth,
                                    json=query)
        response.raise_for_status()
        response_hits = response.json()['hits']['hits']
        if not response_hits:
            logger.warning(f"No entity {type['entity']} found for values: {query_vals}")
            return None
        else:
            return hits_to_records(response_hits, query_vals)

    except Exception as e:
        logger.error(f"Failed to fetch from Elasticsearch: {e}")
        return None


def hit_to_record(hit):
    return {
        key: hit['_source'][key] for key in ['id', 'entity', 'source', 'data_source']
    }

def hits_to_records(hits, query_vals):
    val_to_recs = {}
    ids = []
    for hit in hits:
        for key, vals in query_vals.items():
            prop_val = hit['_source']['properties'].get(key)
            if prop_val:
                record = hit_to_record(hit)
                ids.add(record)
                if isinstance(prop_val, list):
                    for v in prop_val:
                        if v in vals:
                            val_to_recs.setdefault(v, []).append(record)
                else:
                    if prop_val in vals:
                        val_to_recs.setdefault(prop_val, []).append(record)
    logger.info(f"Found {len(ids)} entities for {len(val_to_recs.keys())} types")
    return val_to_recs, unique_dict_list(ids)