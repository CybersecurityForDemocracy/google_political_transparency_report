"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the weekly spend of US spenders from the Google Political Ads bundle

"""

import os
import agate
from dotenv import load_dotenv

load_dotenv()
import records

DB = records.Database()

KEYS = [
    'advertiser_id',
    'advertiser_name',
    'election_cycle',
    'week_start_date',
    'spend_usd'
]

INSERT_QUERY = "INSERT INTO advertiser_weekly_spend ({}) VALUES ({})".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))

def load_advertiser_weekly_spend_to_db(csvfn):
    for row in  agate.Table.from_csv(csvfn):
        if not row["Election_Cycle"] or ('US-Federal' not in row["Election_Cycle"]):
            continue
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        DB.query(INSERT_QUERY, **ad_data)


if __name__ == "__main__":
    csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-advertiser-weekly-spend.csv')
    load_advertiser_weekly_spend_to_db(csvfn)