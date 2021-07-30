"""
script to load from a CSV into SQL db specified as env var DATABASE_URL the ad creative stats from the Google Political Ads bundle

the goal is to *sync* the DB with the CSV, rather than to maintain diffs. that's because the CSV is humongous and much of the data will never change.

"""

import agate

import os
from io import TextIOWrapper, BytesIO
from datetime import datetime, timedelta, date
import logging

import records

from google_political_transparency_report.transparency_bundle.get_transparency_bundle import get_current_bundle, get_zip_file_by_name, get_bundle_date, get_creative_stats_csv
from ..common.post_to_slack import info_to_slack
from ..common.formattimedelta import formattimedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("google_political_transparency_report.transparency_bundle.creative_stats")

KEYS = [
    "ad_id",
    "ad_type",
    "regions",
    "advertiser_id",
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
    'report_date'
]

NUMBER_ABBREVS = {
    "k": 1_000,
    "M": 1_000_000
}
def parse_impressions_string(impressions):
    # options are: ≤ 10k, 10k-100k, 100k-1M, 1M-10M, > 10M
    if "-" in impressions:
        return (int(imp[:-1]) * NUMBER_ABBREVS[imp[-1]] for imp in impressions.split("-"))
    elif impressions == "≤ 10k":
        return 0, 10_000
    elif impressions == "> 10M":
        return 10_000_000, None
    else: 
        print(impressions)
        return None, None


INSERT_QUERY = "INSERT INTO creative_stats ({}) VALUES ({}) ON CONFLICT (ad_id) DO UPDATE SET {}".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]), ', '.join([f"{k} = :{k}" for k in KEYS]))

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


# specifying column types saves 50% of time in loading the CSV! (30min w/o, 15min w/)
CREATIVE_STATS_COLUMN_TYPES = {'Ad_ID': agate.Text(), 'Ad_URL': agate.Text(), 'Ad_Type': agate.Text(), 
                    'Regions': agate.Text(), 'Advertiser_ID': agate.Text(), 'Advertiser_Name': agate.Text(), 
                    'Ad_Campaigns_List': agate.Boolean(), 'Date_Range_Start': agate.Date(), 'Date_Range_End': agate.Date(), 
                    'Num_of_Days': agate.Number(), 'Impressions': agate.Text(), 'Spend_USD': agate.Text(), 
                    'First_Served_Timestamp': agate.DateTime(), 'Last_Served_Timestamp': agate.DateTime(), 
                    'Age_Targeting': agate.Text(), 'Gender_Targeting': agate.Text(), 'Geo_Targeting_Included': agate.Text(), 'Geo_Targeting_Excluded': agate.Text(), 
                    'Spend_Range_Min_USD': agate.Number(), 'Spend_Range_Max_USD': agate.Number(), 'Spend_Range_Min_EUR': agate.Number(), 'Spend_Range_Max_EUR': agate.Number(), 'Spend_Range_Min_INR': agate.Number(), 'Spend_Range_Max_INR': agate.Number(), 'Spend_Range_Min_BGN': agate.Number(), 'Spend_Range_Max_BGN': agate.Number(), 'Spend_Range_Min_HRK': agate.Number(), 'Spend_Range_Max_HRK': agate.Number(), 'Spend_Range_Min_CZK': agate.Number(), 'Spend_Range_Max_CZK': agate.Number(), 'Spend_Range_Min_DKK': agate.Number(), 'Spend_Range_Max_DKK': agate.Number(), 'Spend_Range_Min_HUF': agate.Number(), 'Spend_Range_Max_HUF': agate.Number(), 'Spend_Range_Min_PLN': agate.Number(), 'Spend_Range_Max_PLN': agate.Number(), 'Spend_Range_Min_RON': agate.Number(), 'Spend_Range_Max_RON': agate.Number(), 'Spend_Range_Min_SEK': agate.Number(), 'Spend_Range_Max_SEK': agate.Number(), 'Spend_Range_Min_GBP': agate.Number(), 'Spend_Range_Max_GBP': agate.Number(), 'Spend_Range_Min_NZD': agate.Number(), 'Spend_Range_Max_NZD': agate.Number()}


OLD_CREATIVE_STATS_COLUMN_TYPES = {'Ad_ID': agate.Text(), 'Ad_URL': agate.Text(), 'Ad_Type': agate.Text(), 
                    'Regions': agate.Text(), 'Advertiser_ID': agate.Text(), 'Advertiser_Name': agate.Text(), 
                    'Ad_Campaigns_List': agate.Text(), 'Date_Range_Start': agate.Date(), 'Date_Range_End': agate.Date(), 
                    'Num_of_Days': agate.Number(), 'Impressions': agate.Text(), 'Spend_USD': agate.Text(), 
                    }


CREATIVE_STATS_SCHEMA_CHANGE_DATE = date(2020, 7, 1) # it's sometime around here, I don't know for sure, that the schema changes

def load_creative_stats_to_db(csvfn, report_date):
    DB = records.Database(os.environ['DATABASE_URL'])

    # should log: duration, total rows; New today; here yesterday, gone today.
    total_rows_today = 0

    # start_time_pre_counting = datetime.now()
    rows_in_yesterdays_report = DB.query("""SELECT count(*) as count FROM creative_stats WHERE report_date = (SELECT max(report_date) from creative_stats where report_date != :report_date);""", report_date=report_date)[0]["count"]
    # duration_pre_counting = (datetime.now() - start_time_pre_counting)

    start_time_data_loading = datetime.now()
    for batch in  chunks(agate.Table.from_csv(csvfn, column_types=(CREATIVE_STATS_COLUMN_TYPES if report_date > CREATIVE_STATS_SCHEMA_CHANGE_DATE else OLD_CREATIVE_STATS_COLUMN_TYPES)), 100):
        ads_data = []
        for row in batch:
            total_rows_today += 1
            ad_data = {k.lower():v for k,v in row.items() if k.lower() in KEYS}
            for k in KEYS: 
                ad_data[k] = ad_data.get(k, None)
            ad_data["impressions_min"], ad_data["impressions_max"] = parse_impressions_string(row["Impressions"])
            ad_data["spend_usd"] = ad_data["spend_usd"] or 0
            ad_data["report_date"] = report_date
            ads_data.append(ad_data)
        DB.bulk_query(INSERT_QUERY, ads_data)
    duration_data_loading = (datetime.now() - start_time_data_loading)

    # start_time_post_counting = datetime.now()
    rows_present_yesterday_but_missing_today = DB.query("""SELECT count(*) as count FROM creative_stats WHERE report_date = :report_date;""", report_date = report_date - timedelta(days=1))[0]["count"]
    new_rows_today = total_rows_today - (rows_in_yesterdays_report - rows_present_yesterday_but_missing_today) 
    # duration_post_counting = (datetime.now() - start_time_post_counting)

    log1 = "loading creative_stats report took {} to load {} total rows".format(formattimedelta(duration_data_loading), total_rows_today)
    log2 = "creative stats: {} new rows; {} missing that were present yesterday".format(new_rows_today, rows_present_yesterday_but_missing_today)
    log.info(log1)
    log.info(log2)
    info_to_slack("Google ads: " + log1 + "\n" + log2)


if __name__ == "__main__":
    with get_current_bundle() as zip_file:
        bundle_date = get_bundle_date(zip_file)
        load_creative_stats_to_db(TextIOWrapper(BytesIO(get_creative_stats_csv(zip_file))), bundle_date)
