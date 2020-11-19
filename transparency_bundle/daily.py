# we want to get the bundle daily
# upload the (daily) advertiser stats CSV to Google Cloud
# insert the  (daily) advertiser stats to the DB
# sync the latest advertiser weekly spend to the DB
# sync the creative stats to the DB

import os
from io import TextIOWrapper, BytesIO

from get_transparency_bundle import get_current_bundle, upload_advertiser_stats_from_bundle, get_advertiser_weekly_spend_csv, get_creative_stats_csv, get_advertiser_stats_csv, get_bundle_date
from load_advertiser_weekly_spend import load_advertiser_weekly_spend_to_db
from load_advertiser_stats import load_advertiser_stats_to_db
from load_creative_stats import load_creative_stats_to_db

if __name__ == "__main__":
    local_dest_for_bundle = os.path.join(os.path.dirname(__file__), '..', 'data')
    with get_current_bundle() as zip_file:
        bundle_date = get_bundle_date(zip_file)
        upload_advertiser_stats_from_bundle(zip_file, local_dest_for_bundle)
        load_advertiser_weekly_spend_to_db(TextIOWrapper(BytesIO(get_advertiser_weekly_spend_csv(zip_file))))
        load_advertiser_stats_to_db(TextIOWrapper(BytesIO(get_advertiser_stats_csv(zip_file))), bundle_date)
        load_creative_stats_to_db(TextIOWrapper(BytesIO(get_creative_stats_csv(zip_file))))
