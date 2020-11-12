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


INSERT_QUERY = "INSERT INTO advertiser_stats ({}) VALUES ({})".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))


csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-advertiser-stats.csv')
def load_advertiser_stats_to_db():
for row in  agate.Table.from_csv(csvfn):
    if not row["Elections"]  or row["Elections"] != 'US-Federal':
        continue
    ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
    ad_data["spend_usd"] = ad_data["spend_usd"] or 0
    DB.query(INSERT_QUERY, **ad_data)


if __name__ == "__main__":
    csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv')
    load_advertiser_stats_to_db(csvfn):