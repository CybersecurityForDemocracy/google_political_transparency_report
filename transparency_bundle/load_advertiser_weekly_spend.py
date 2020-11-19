"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the weekly spend of US spenders from the Google Political Ads bundle

"""

import os
import agate
from dotenv import load_dotenv
from io import StringIO

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

INSERT_QUERY = "INSERT INTO google.advertiser_weekly_spend ({}) VALUES ({}) ON CONFLICT (advertiser_id, week_start_date)  DO NOTHING".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))

def load_advertiser_weekly_spend_to_db(csv_filelike):
    for row in  agate.Table.from_csv(csv_filelike):
        if not row["Election_Cycle"] or ('US-Federal' not in row["Election_Cycle"]):
            continue
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        DB.query(INSERT_QUERY, **ad_data)


if __name__ == "__main__":
    # csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-advertiser-weekly-spend.csv')
    # with open(csvfn, 'r') as f:
        # load_advertiser_weekly_spend_to_db(f)
    local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', 'data')
    with get_current_bundle() as zip_file:
        bundle_date = get_bundle_date(zip_file)
        load_advertiser_weekly_spend_to_db(TextIOWrapper(BytesIO(get_advertiser_weekly_spend_csv(zip_file))))
