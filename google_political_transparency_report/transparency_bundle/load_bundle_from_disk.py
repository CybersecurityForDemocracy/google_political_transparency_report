# for a bundle Zip we already have, we want to process it like it was the daily report fetched with daily.py
# upload the (daily) advertiser stats CSV to Google Cloud
# insert the  (daily) advertiser stats to the DB
# sync the latest advertiser weekly spend to the DB
# sync the creative stats to the DB

import os
from io import TextIOWrapper, BytesIO
import logging
import tempfile
from sys import argv
import datetime

from google_political_transparency_report.transparency_bundle.get_transparency_bundle import get_current_bundle, upload_advertiser_stats_from_bundle, get_advertiser_weekly_spend_csv, get_creative_stats_csv, get_advertiser_stats_csv, get_bundle_date
from google_political_transparency_report.transparency_bundle.load_advertiser_weekly_spend import load_advertiser_weekly_spend_to_db
from google_political_transparency_report.transparency_bundle.load_advertiser_stats import load_advertiser_stats_to_db
from google_political_transparency_report.transparency_bundle.load_creative_stats import load_creative_stats_to_db
from google_political_transparency_report.transparency_bundle.load_advertiser_regional_spend import load_advertiser_regional_spend_to_db
from google_political_transparency_report.common.post_to_slack import warn_to_slack


def get_bundle_from_zip(zip_fn):
    return open(zip_fn, 'rb')


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("google_political_transparency_report.transparency_bundle.daily")

if __name__ == "__main__":
    try: 
        with tempfile.TemporaryDirectory() as local_dest_for_bundle:
        # local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
            with get_bundle_from_zip(argv[1]) as zip_file:
                explicit_bundle_date = datetime.date(*map(int, argv[2].split("-"))) if len(argv) >= 3 else None
                bundle_date = explicit_bundle_date or get_bundle_date(zip_file)
                # NOTE: we're not loading advertiser_stats b/c old data (what we're loading here) would squash newer data already in the DB.
                #                 assert False upload_advertiser_stats_from_bundle(zip_file, local_dest_for_bundle, bundle_date)
                #                 assert False  load_advertiser_stats_to_db(TextIOWrapper(BytesIO(get_advertiser_stats_csv(zip_file))), bundle_date)
                load_advertiser_weekly_spend_to_db(TextIOWrapper(BytesIO(get_advertiser_weekly_spend_csv(zip_file))))
                # NOTE: we're not doing creative_stats, since it overwrites stuff, which is a problem if you're loading an old bundle                 load_creative_stats_to_db(TextIOWrapper(BytesIO(get_creative_stats_csv(zip_file))), bundle_date)
                load_advertiser_regional_spend_to_db(TextIOWrapper(BytesIO(get_creative_stats_csv(zip_file))), bundle_date)
    except Exception as e:
        warn_to_slack(f"google_political_transparency_report.transparency_bundle.daily error: {e}")
        log.error(e)
        raise e
