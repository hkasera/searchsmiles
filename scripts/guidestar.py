import requests, json

EXCHANGE_NEEDED_FIELDS = set([
    'organization_email',
    'organization_blog',
    'government_issued_id',
    'keywords',
    'primary_address',
    'primary_telephone',
    'international_phone',
    'logo_name',
    'logo_path',
    'photos',
    'videos',
    'social',
    'participation_level',
    'irs_subsection_code',
    'irs_509a_status',
    'bridge_id',
    'approval_date',
    'funding_sources',
    'funding_needs',
])

DETAIL_NEEDED_FIELDS = set([
    'ein',
    'organization_id',
    'organization_name',
    'aka_organization_name',
    'year_founded',
    'ruling_year',
    'tax_year',
    'revenue_total',
    'income_total',
    'expenses_total',
    'assets',
    'website',
    'report_page',
    'mission',
    'foundation_code',
    'foundation_code_description',
    'affiliation_code',
    'affiliation_code_description',
    'deductibility_code',
    'deductibility_code_description',
    'subsection_code',
    'subsection_code_description',
    'address_line1',
    'address_line2',
    'city',
    'state',
    'zip',
    'zip4',
    'contact_phone',
    'telephone',
])

def get_exchange(org_id):
    r = requests.get('https://sandboxdata.guidestar.org/v3/exchange/{0}.json'.format(org_id), auth=('AUTH', ''))
    return r.json() if r.status_code == requests.codes.ok else None

def get_detail(org_id):
    r = requests.get('https://sandboxdata.guidestar.org/v1/detail/{0}.json'.format(org_id), auth=('AUTH', ''))
    return r.json() if r.status_code == requests.codes.ok else None

def get_address(item):
    parts = filter(lambda x: x, [
        item['address_line1'],
        item['address_line2'],
        item['city'],
        item['state'],
        item['zip'],
    ])

    address = ', '.join(parts)

    if item['zip'] and item['zip4']:
        address += '-' + item['zip4']

    return address

def get(org_id):
    d_detail = get_detail(org_id)

    if d_detail is None:
        return None

    item =  { k:v for k,v in d_detail.iteritems() if k in DETAIL_NEEDED_FIELDS }
    item['geographic_areas_served'] = map(lambda x: x['area_served'], d_detail['geographic_areas_served'])
    item['organization_ntee_codes'] = map(lambda x: x['ntee_code_details']['nteecodedescription'], d_detail['organization_ntee_codes'])
    item['full_address'] = get_address(item)
    item['year_founded'] = item['year_founded'].strip() if item['year_founded'] else None

    d_exchange = get_exchange(org_id)
    if d_exchange is not None:
        item['exchange'] = { k:v for k,v in d_exchange.iteritems() if k in EXCHANGE_NEEDED_FIELDS }

    return item

# print json.dumps(get(8347195), indent=4)
