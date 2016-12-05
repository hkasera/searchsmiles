import json
import time
import csv
import re
import guidestar
import geocoding
import batch
from util import deep_get, deep_try_get, deep_set, clean_text, clean_list
from dateutil.parser import parse as parse_date

def update_guidestar():
    def get_action(hit):
        org_id = hit['_source']['source_data']['organization_id']
        gs_data = None
        retry_count = 0

        while not gs_data and retry_count <= 10:
            try:
                gs_data = guidestar.get(org_id)
            except Exception as ex:
                print org_id, 'Error:', ex

            retry_count += 1
            if not gs_data:
                print org_id, 'Not Found'
                time.sleep(5)

        print org_id, 'Found' if gs_data else 'Really Not Found'
        return batch.get_update_action(hit['_id'], { 'gs_data': gs_data })

    batch.run({
        '_source': ['source_data.organization_id'],
        'query': {
            'bool': {
                'must': [
                    {'exists': {'field': 'source_data.organization_id'}},
                    {'match': { "gs_data.not_found": { "query": True }}}
                ]
            },
        }
    }, get_action)

def update_address():
    def get_action(hit):
        address = hit['_source']['gs_data']['full_address']
        return batch.get_update_action(hit, { 'address': address, 'address_updated': True })

    batch.run({
        '_source': ['gs_data.full_address'],
        'query': {
            'type': 'ngo',
            'bool': {
                'must': [{ 'exists': { 'field': 'gs_data.full_address' } }],
                'must_not': [{ 'exists': { 'field': 'address_updated' } }]
            }
        }
    }, get_action)

def copy_data():
    def get_action(hit):
        return {
            '_index': 'ngos',
            '_type': 'ngo',
            '_id': hit['_id'],
            '_source': hit['_source']
        }

    batch.run({
        'query': {
            'match_all': {}
        }
    }, get_action)

def update_coords():
    def get_action(hit):
        address = clean_text(hit['_source']['address'])

        try:
            results = geocoding.get(address)
        except Exception as ex:
            print address, 'Error:', ex
            return batch.get_update_action(hit, {
                'address': address,
                'geocoding_data': None,
                'location_error': 'NotFound'
            })

        n = len(results)
        doc = {}

        if n > 0:
            geo = results[0]
            doc = {
                'formatted_address': geo['formatted_address'],
                'location': {
                    'lat': geo['geometry']['location']['lat'],
                    'lon': geo['geometry']['location']['lng']
                }
            }

        if n != 1:
            doc['location_error'] = 'NotFound' if n == 0 else 'Ambiguous'

        doc['geocoding_data'] = results
        doc['address'] = address

        return batch.get_update_action(hit, doc)

    batch.run({
        'query': {
            'bool': {
                'must': [
                    { 'type': { 'value': 'ngo' } },
                    { 'wildcard': { 'address': '*' } }
                ],
                'must_not': [
                    { 'exists': { 'field': 'location' } },
                    { 'exists': { 'field': 'location_error' } }
                ]
            }
        }
    }, get_action);

def transform(src, dest):
    if isinstance(src, dict):
        for k in src.keys():
            if k in dest:
                src[k] = transform(src[k], dest[k])
            else:
                del src[k]
    elif isinstance(src, list):
        if len(dest) > 0:
            src = [transform(x, dest[0]) for x in src]
    return src

def restore_data():
    def clean(src):
        src['name'] = clean_text(src['name'])
        src['mission'] = deep_try_get(src, 'mission', 'gs_data.mission', 'fb_data.mission')
        src['city'] = deep_try_get(src, 'city', 'source_data.city', 'gs_data.city')
        src['state'] = deep_try_get(src, 'state', 'gs_data.state')
        src['link'] = deep_try_get(src, 'link', 'source_data.website','gs_data.website')

        keywords = deep_try_get(src, 'keywords', 'gs_data.exchange.keywords')
        if keywords:
            src['keywords'] = clean_list(keywords.split(','))

        if 'areas' in src:
            src['areas'] = clean_list(src['areas'])

        value = deep_get(src, 'gs_data.geographic_areas_served')
        if value:
            deep_set(src, 'gs_data.geographic_areas_served', clean_list(value))

        value = deep_get(src, 'gs_data.organization_ntee_codes')
        if value:
            deep_set(src, 'gs_data.organization_ntee_codes', clean_list(value))

        value = deep_get(src, 'tw_data.created_at')
        if value:
            deep_set(src, 'tw_data.created_at', parse_date(value).isoformat())

        value = deep_get(src, 'tw_data.status.created_at')
        if value:
            deep_set(src, 'tw_data.last_update_at', parse_date(value).isoformat())
            del src['tw_data']['status']['created_at']

        value = deep_get(src, 'fb_data.location')
        if value:
            deep_set(src, 'fb_data.address', value)
            del src['fb_data']['location']

        value = deep_get(src, 'fb_data.address.latitude')
        if value:
            deep_set(src, 'fb_data.location.lat', value)

        value = deep_get(src, 'fb_data.address.longitude')
        if value:
            deep_set(src, 'fb_data.location.lon', value)

        if 'isSiteDown' in src:
            src['is_site_down'] = src['isSiteDown']
            del src['isSiteDown']

    schema = json.load(open('../schema.json', 'r'))

    def get_action(hit):
        src = transform(hit['_source'], schema)
        clean(src)

        return {
            '_index': hit['_index'],
            '_type': hit['_type'],
            '_id': hit['_id'],
            '_source': hit['_source']
        }

    batch.restore(get_action)

def upload_events():
    def clean(event):
        location = deep_get(event, 'place.location')
        if location:
            event['location'] = {
                'lat': location['latitude'],
                'lon': location['longitude']
            }

            if not location['latitude'] or not location['longitude']:
                print 'Yo!'

        return event

    def get_action(hit):
        events = deep_get(hit, '_source.fb_data.events.data')

        return ({
            '_index': hit['_index'],
            '_type': 'event',
            '_parent': hit['_id'],
            '_source': clean(event)
        } for event in events)

    batch.restore(get_action)

def update_source():
    def get_action(hit):
        if 'source_link' not in hit['_source']:
            print hit

        source_link = hit['_source']['source_link']
        source = None

        if 'guidestar' in source_link:
            source = 'guidestar'
        elif 'unodc' in source_link:
            source = 'unodc'

        return batch.get_update_action(hit, { 'source': source })

    batch.run({
        '_source': ['source_link'],
        'query': {
            'bool': {
                'must': [{ 'type': { 'value': 'ngo' } }],
                'must_not': [{ 'exists': { 'field': 'source' } }]
            }
        }
    }, get_action)

def clean_address():
    def get_action(hit):
        address = hit['_source']['address']
        m = re.search(r'.*[\r\n\t\s]+([\w\W]+?)(tel|EIN):', address, re.M)
        address = clean_text(m.group(1))
        return batch.get_update_action(hit, { 'address': address })

    batch.run({
        '_source': ['address'],
        'query': {
            'bool': {
                'must': [
                    { 'type': { 'value': 'ngo' } },
                    { 'term': { 'source': 'charity_nav' } }
                ]
            }
        }
    }, get_action)

def copy_fb_location():
    def get_action(hit):
        location = deep_get(hit, '_source.fb_data.location')
        return batch.get_update_action(hit, { 'location': location })

    batch.run({
        '_source': ['fb_data.location'],
        'query': {
            'bool': {
                'must': [
                    { 'type': { 'value': 'ngo' } },
                    { 'exists': { 'field': 'fb_data.location' } },
                ],
                'must_not': [
                    { 'exists': { 'field': 'location' } }
                ]
            }
        }
    }, get_action)

def more_coords():
    def get_action(hit):
        results = hit['_source']['geocoding_data']
        doc = {}

        if len(results) > 1:
            geo = results[0]
            doc = {
                'formatted_address': geo['formatted_address'],
                'location': {
                    'lat': geo['geometry']['location']['lat'],
                    'lon': geo['geometry']['location']['lng']
                }
            }

        return batch.get_update_action(hit, doc)

    batch.restore(get_action)

def get_component(geo, component_name):
    return next((x['short_name'] for x in geo['address_components'] if component_name in x['types']), None)

def update_city():
    with open('../data/location.jsonl', 'r') as f:
        locations = (json.loads(line) for line in f)
        loc_dict = { x['_id']:x['geocoding_data'] for x in locations }

    def get_action(hit):
        src = hit['_source']
        geos = loc_dict.get(hit['_id'], None)
        if geos and len(geos) > 0 and len(src.get('state') or '') > 2:
            geo = geos[0]
            doc = {
                'state': get_component(geo, 'administrative_area_level_1'),
            }
        else:
            return []

        return [batch.get_update_action(hit, doc)]

    batch.run({
        '_source': ['state'],
        'query': {
            'bool': {
                'must': [
                    { 'type': { 'value': 'ngo' } },
                    { 'exists': { 'field': 'location' } }
                ]
            }
        }
    }, get_action)

def export_csv():
    field_name = 'areas'
    with open('../dedup/{}.csv'.format(field_name), 'wb') as f:
        fieldnames = [field_name]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        es = batch.get_es()
        r = es.search(index='ngos', body={
            "size": 0,
            "aggs": {
                "values": {
                    "terms": {
                        "field": field_name,
                        "size": 100000
                    }
                }
            }
        })

        values = sorted([x['key'].encode('utf-8') for x in r['aggregations']['values']['buckets']])
        for v in values:
            writer.writerow({field_name: v})

def main():
    export_csv()
    pass

main()
