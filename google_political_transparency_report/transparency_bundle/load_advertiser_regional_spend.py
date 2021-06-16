
"""

script to load from a CSV into SQL db specified as env var DATABASE_URL the weekly spend of US spenders from the Google Political Ads bundle

what's tricky about this is that we need to see how spend changes over time
and Google just gives us a total, that changes daily-ish.

options:

1. we could keep everything... but that might take up a lot of space
2. we could keep only the changes... but there might be flutter AND that's more complicated to deal with downstream
3. we could only keep weekly records
"""

# Advertiser_ID,Advertiser_Name,Country,Country_Subdivision_Primary,Spend_USD,Spend_EUR,Spend_INR,Spend_BGN,Spend_HRK,Spend_CZK,Spend_DKK,Spend_HUF,Spend_PLN,Spend_RON,Spend_SEK,Spend_GBP,Spend_ILS,Spend_NZD
# AR100201587015680000,ALEX FOR AZ,US,WI,0,0,500,25,100,0,0,0,0,0,0,0,0,0
# AR100201587015680000,ALEX FOR AZ,US,UT,0,0,250,0,0,0,0,0,0,0,0,0,0,0

import os
from datetime import datetime, timedelta
from io import TextIOWrapper, BytesIO

import agate
from dotenv import load_dotenv

from .get_transparency_bundle import get_current_bundle, get_bundle_date, get_advertiser_regional_spend_csv
from ..common.post_to_slack import info_to_slack
from ..common.formattimedelta import formattimedelta

load_dotenv()
import records
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("google_political_transparency_report.transparency_bundle.advertiser_regional_spend")

DB = records.Database()

KEYS = [
    "advertiser_id",
    "country",
    "region",
    "spend_usd",
    "report_date"
]

## TODO
INSERT_QUERY = "INSERT INTO advertiser_regional_spend ({}) VALUES ({}) ON CONFLICT (advertiser_id, country, region, report_date)  DO NOTHING".format(', '.join([(k) for k in KEYS]), ', '.join([":" + (k) for k in KEYS]))

def load_advertiser_regional_spend_to_db(csv_filelike, bundle_date):
    MAX_REPORT_DATE = DB.query("SELECT max(report_date) report_date FROM advertiser_regional_spend;")[0]["report_date"]
    if bundle_date == MAX_REPORT_DATE:
        return
    # load CSV to DB
    # delete
    total_rows = 0
    start_time = datetime.now()
    for row in  agate.Table.from_csv(csv_filelike):
        if "Country" not in row.keys() or row["Country"] != 'US': # this doesn't exist for the EU, oddly!
            continue
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        ad_data["region"] = row["Country_Subdivision_Primary"]
        ad_data["report_date"] = bundle_date
        total_rows += 1
        DB.query(INSERT_QUERY, **ad_data)

    duration = (datetime.now() - start_time)
    log1 = "loaded {} advertiser regional spend records for this week in {}".format(total_rows , formattimedelta(duration))
    log.info(log1)
    info_to_slack("Google ads: " + log1)


if __name__ == "__main__":
    # csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-advertiser-weekly-spend.csv')
    # with open(csvfn, 'r') as f:
        # load_advertiser_weekly_spend_to_db(f)
    from sys import argv
    def get_bundle_from_zip(zip_fn):
        return open(zip_fn, 'rb')

    local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', 'data')
#    with get_current_bundle() as zip_file:
    with get_bundle_from_zip(argv[1]) as zip_file:
        explicit_bundle_date = datetime.date(*map(int, argv[2].split("-"))) if len(argv) >= 3 else None
        bundle_date = explicit_bundle_date or get_bundle_date(zip_file)
        load_advertiser_regional_spend_to_db(TextIOWrapper(BytesIO(get_advertiser_regional_spend_csv(zip_file))), bundle_date)
