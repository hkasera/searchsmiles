from elasticsearch import Elasticsearch, helpers
from multiprocessing.dummy import Pool
# from multiprocess import Pool
import dill
import itertools, json, os, math
from time import gmtime, strftime
from util import deep_get, deep_try_get, deep_set, clean_list

BACKUP_FOLDER = '../backup/'

THREAD_COUNT = 20
CHUNK_SIZE = 100
BULK_SIZE = CHUNK_SIZE*THREAD_COUNT

INDEX_NAME = 'ngos'
TYPE_NAME = 'ngo'

es_remote = Elasticsearch('http://767538a80e2cb56a5115c050db18700d.us-west-1.aws.found.io:9200', http_auth=('elastic', 'jmZEm0ASzrp81BAOmTU95bj9'))
es_remote_new = Elasticsearch('http://6ababf46412e5cd9ff51a87e3b704981.us-west-1.aws.found.io:9200', http_auth=('elastic', 'QHtaXf3aNCwJtuz66nKZfnRq'))
es_local = Elasticsearch('http://localhost:9200')
es = es_remote_new

def get_es():
    return es

def refresh_index():
    es.indices.refresh(index=INDEX_NAME)

def start_bulk():
    es.indices.put_settings(index=INDEX_NAME, body={ "index": { "refresh_interval": "-1", "number_of_replicas": 0 } })

def end_bulk():
    es.indices.forcemerge(index=INDEX_NAME, max_num_segments=5)
    es.indices.put_settings(index=INDEX_NAME, body={ "index": { "refresh_interval": "1s", "number_of_replicas": 0 } })

def remove_field(field_name):
    path, field = ('.' + field_name).rsplit('.', 1)
    r = es.update_by_query(index=INDEX_NAME, request_timeout=300, body={
        'query': { 'exists': { 'field': field_name } },
        'script': { 'inline': 'ctx._source{}.remove("{}")'.format(path, field) }
    })
    print r

def get_update_action(hit, obj):
    return {
        '_op_type': 'update',
        '_index': INDEX_NAME,
        '_type': TYPE_NAME,
        '_id': hit['_id'],
        'doc': obj
    }

def backup():
    hits = helpers.scan(es, index=INDEX_NAME, size=5000)
    suffix = strftime("%Y%m%d-%H%M%S", gmtime())
    with open(BACKUP_FOLDER + 'data-{}.jsonl'.format(suffix), 'w') as f:
        for hit in hits:
            f.write(json.dumps(hit) + '\n')

def restore(toAction):
    start_bulk()

    last_backup_file = sorted([f for f in os.listdir(BACKUP_FOLDER) if f.endswith('.jsonl')], reverse=True)[0]
    with open(BACKUP_FOLDER + last_backup_file, 'r') as f:
        items = (json.loads(l) for l in f)

        items = (item for item in items if len(deep_get(item, '_source.geocoding_data', [])) > 0)
        # items = itertools.islice(items, 10)

        actions = (a for x in items for a in toAction(x))
        # actions = (toAction(x) for x in items)

        print len(list(actions))
        for a in actions:
            print json.dumps(a, indent=4)
        return

        results = helpers.streaming_bulk(es, actions, chunk_size=BULK_SIZE)

        counter = 0
        for result in results:
            counter += 1
            if counter % BULK_SIZE == 0:
                print 'Updated:', counter, result

    end_bulk()

def run(query, toAction):
    start_bulk()

    r = es.search(index=INDEX_NAME, body=query)
    total = r['hits']['total']
    print 'Total:', total

    hits = helpers.scan(es, index=INDEX_NAME, query=query)
    limit = 1000000
    hits = itertools.islice(hits, limit)
    chunk_size = max(1, min(CHUNK_SIZE, math.floor(min(total, limit) / THREAD_COUNT)))

    # pool = Pool(THREAD_COUNT)
    # actions = pool.imap_unordered(toAction, hits, chunksize=chunk_size)

    # f = open('../data/location.jsonl', 'a')
    # def do_action(action):
    #     f.write(json.dumps({
    #         '_id': action['_id'],
    #         'address': action['doc']['address'],
    #         'geocoding_data': action['doc']['geocoding_data'],
    #     }) + '\n')

    #     del action['doc']['address']
    #     del action['doc']['geocoding_data']
    #     return action

    # actions = (do_action(a) for a in actions)
    # actions = (toAction(x) for x in hits)
    actions = (a for x in hits for a in toAction(x))

    for a in actions:
        # continue
        print json.dumps(a)
    return

    results = helpers.streaming_bulk(es, actions, chunk_size=BULK_SIZE)

    counter = 0
    for result in results:
        counter += 1
        if counter % BULK_SIZE == 0:
            print 'Updated:', counter, result

    end_bulk()

def main():
    r = es.search(index=INDEX_NAME, body={
        "query": {
            "wildcard": {
              "keywords": "\"*\""
            }
        }
    })

    print r['hits']['total']
    return
    print es.update_by_query(index=INDEX_NAME, request_timeout=300, body={
      "script": {
        "lang": "painless",
        "inline": "ctx._source.keywords.removeAll(Collections.singleton(params.value)); ctx._source.keywords.add(params.new_value);",
        "params": {
          "value": "\"abortion\"",
          "new_value": "abortion"
        }
      },
      "query": {
        "term": {
          "keywords": "\"*\""
        }
      }
    })

# main()


