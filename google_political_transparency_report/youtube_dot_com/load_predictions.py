import csv
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
import records

DB = records.Database()


QUERY = "INSERT INTO political_values (model_id, youtube_ad_id, political_value) VALUES (:model_id, :youtube_ad_id, :political_value) ON CONFLICT DO NOTHING"

for i, row in pd.read_csv(os.path.join(os.path.dirname(__file__), "youtube_video_subs_with_political_rankings.csv")).iterrows():
    DB.query(QUERY, model_id=0, youtube_ad_id=row["id"], political_value=row["prediction"])