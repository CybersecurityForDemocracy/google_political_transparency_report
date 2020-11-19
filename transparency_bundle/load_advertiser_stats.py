"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the advertiser stats of US spenders from the Google Political Ads bundle

"""

import agate
from dotenv import load_dotenv
import os

load_dotenv()
import records

DB = records.Database()

KEYS = [
    'advertiser_id',
    'advertiser_name',
    'public_ids_list',
    'regions',
    'elections',
    'total_creatives',
    'spend_usd'
]
EXTRA_KEYS = ['report_date']


INSERT_QUERY = "INSERT INTO google.advertiser_stats ({}) VALUES ({}) ON CONFLICT (advertiser_id) DO UPDATE SET {}".format(', '.join([k for k in KEYS + EXTRA_KEYS]), ', '.join([":" + k for k in KEYS + EXTRA_KEYS]), ', '.join([f"{k} = :{k}" for k in KEYS + EXTRA_KEYS]))

def load_advertiser_stats_to_db(csvfn, date):
    for row in  agate.Table.from_csv(csvfn):
        if not row["Elections"]  or row["Elections"] != 'US-Federal':
            continue
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        ad_data["report_date"] = date
        DB.query(INSERT_QUERY, **ad_data)


if __name__ == "__main__":
    # csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv')
    local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', 'data')
    with get_current_bundle() as zip_file:
        bundle_date = get_bundle_date(zip_file)
        load_advertiser_stats_to_db(TextIOWrapper(BytesIO(get_advertiser_stats_csv(zip_file))), bundle_date)
