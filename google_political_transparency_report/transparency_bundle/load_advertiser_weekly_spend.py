"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the weekly spend of US spenders from the Google Political Ads bundle

"""

import os
from datetime import datetime, timedelta
from io import TextIOWrapper, BytesIO

import agate
from dotenv import load_dotenv

from .get_transparency_bundle import get_current_bundle, get_bundle_date, get_advertiser_weekly_spend_csv
from ..common.post_to_slack import info_to_slack
from ..common.formattimedelta import formattimedelta

load_dotenv()
import records
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("google_political_transparency_report.transparency_bundle.advertiser_weekly_spend")

DB = records.Database()

KEYS = [
    'advertiser_id',
    'advertiser_name',
    'election_cycle',
    'week_start_date',
    'spend_usd'
]

INSERT_QUERY = "INSERT INTO advertiser_weekly_spend ({}) VALUES ({}) ON CONFLICT (advertiser_id, week_start_date)  DO NOTHING".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))

def load_advertiser_weekly_spend_to_db(csv_filelike):
    total_rows = 0
    start_time = datetime.now()
    for row in  agate.Table.from_csv(csv_filelike):
        if not row["Election_Cycle"] or ('US-Federal' not in row["Election_Cycle"]):
            continue
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        total_rows += 1
        DB.query(INSERT_QUERY, **ad_data)
    duration = (datetime.now() - start_time)
    log1 = "loaded {} advertiser weekly spend records for this week in {}".format(total_rows , formattimedelta(duration))
    log.info(log1)
    info_to_slack("Google ads: " + log1)

if __name__ == "__main__":
    # csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-advertiser-weekly-spend.csv')
    # with open(csvfn, 'r') as f:
        # load_advertiser_weekly_spend_to_db(f)
    local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', 'data')
    with get_current_bundle() as zip_file:
        bundle_date = get_bundle_date(zip_file)
        load_advertiser_weekly_spend_to_db(TextIOWrapper(BytesIO(get_advertiser_weekly_spend_csv(zip_file))))
