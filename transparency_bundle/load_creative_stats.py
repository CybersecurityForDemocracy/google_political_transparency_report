"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the ad creative stats from the Google Political Ads bundle

"""

import agate
from dotenv import load_dotenv
import os

load_dotenv()
import records

from get_transparency_bundle import get_current_bundle, get_zip_file_by_name


DB = records.Database()

KEYS = [
    "ad_id",
    "ad_url",
    "ad_type",
    "regions",
    "advertiser_id",
    "advertiser_name",
    "date_range_start",
    "date_range_end",
    "num_of_days",
    "spend_usd",
    "first_served_timestamp",
    "last_served_timestamp",
    "age_targeting",
    "gender_targeting",
    "geo_targeting_included",
    "geo_targeting_excluded",
    "spend_range_min_usd",
    "spend_range_max_usd",
    "impressions_min",
    "impressions_max",
]

NUMBER_ABBREVS = {
    "k": 1_000,
    "M": 1_000_000
}
def parse_impressions_string(impressions):
    # ≤ 10k
    # 10k-100k
    # 100k-1M
    # 1M-10M
    # > 10M
    if "-" in impressions:
        return (int(imp[:-1]) * NUMBER_ABBREVS[imp[-1]] for imp in impressions.split("-"))
    elif impressions == "≤ 10k":
        return 0, 10_000
    elif impressions == "> 10M":
        return 10_000_000, None
    else: 
        print(impressions)
        return None, None


INSERT_QUERY = "INSERT INTO creative_stats ({}) VALUES ({}) ON CONFLICT UPDATE".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))

def load_creative_stats_to_db(csvfn):
    for row in  agate.Table.from_csv(csvfn):
        ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
        ad_data["impressions_min"], ad_data["impressions_max"] = parse_impressions_string(row["Impressions"])
        ad_data["spend_usd"] = ad_data["spend_usd"] or 0
        DB.query(INSERT_QUERY, **ad_data)


if __name__ == "__main__":
    with get_current_bundle() as bundle:
        csv = get_zip_file_by_name(bundle, "google-political-ads-creative-stats.csv")
        csvfn = os.path.join(os.path.dirname(__file__), '..', 'data/google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv')
        load_creative_stats_to_db(csvfn)