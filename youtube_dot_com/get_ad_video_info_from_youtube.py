
import csv

import requests
import webvtt
import youtube_dl
from dotenv import load_dotenv
from io import StringIO
from time import sleep

load_dotenv()
import records

DB = records.Database()

ydl = youtube_dl.YoutubeDL({
    'outtmpl': '%(id)s.%(ext)s',
    'writeautomaticsub': True,
    'subtitleslangs': ['en'],
})

KEYS = [
    "id", 
    "uploader", 
    "uploader_id", 
    "uploader_url", 
    "channel_id", 
    "channel_url", 
    "upload_date", 
    "license", 
    "creator", 
    "title", 
    "alt_title", 
    "thumbnail", 
    "description", 
    "categories", 
    "tags", 
    "duration", 
    "age_limit", 
    "webpage_url", 
    "view_count", 
    "like_count", 
    "dislike_count", 
    "average_rating", 
    "is_live", 
    "display_id", 
    "format", 
    "format_id", 
    "width", 
    "height", 
    "resolution", 
    "fps", 
    # "fulltitle", 
    "subs",
    "subtitle_lang",
    "error",
]
INSERT_QUERY = "INSERT INTO google.youtube_videos ({}) VALUES ({})".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]))


scraped_youtube_video_ads = DB.query("select distinct youtube_ad_id from google.google_ad_creatives left outer join youtube_videos on youtube_videos.id = youtube_ad_id where youtube_ad_id is not null and (youtube_videos.id is null or youtube_videos.error = true)")
# observed_youtube_video_ads = DB.query("select platformitemid from observations.youtube_ads where paid_for_by is not null")
ads = scraped_youtube_video_ads # + observed_youtube_video_ads
with ydl:
    for ad in ads:
        while True:
            retried = False
            try:
                video = ydl.extract_info(
                    f'http://www.youtube.com/watch?v={ad.youtube_ad_id}',
                    download=False # We just want to extract the info
                )
            except (youtube_dl.utils.ExtractorError, youtube_dl.utils.DownloadError):
                if retried:
                    DB.query(INSERT_QUERY, **{"id": ad.youtube_ad_id, "error": True})
                    break
                else:
                    sleep(5)
                    print("retrying")
                    retried = True
                    continue

            if video['requested_subtitles'] and "en" in video['requested_subtitles']:
                subtitle_data = requests.get(video['requested_subtitles']['en']['url'], stream=True).text
                subtitle_lang = "en"
            elif video['requested_subtitles'] and "es" in video['requested_subtitles']:
                subtitle_data = requests.get(video['requested_subtitles']['es']['url'], stream=True).text
                subtitle_lang = "es"
            else:
                subtitle_data = None
                subtitle_lang = None

            if subtitle_data:
                subtitle_lines = [caption.text for caption in webvtt.read_buffer(StringIO(subtitle_data))]
                subtitle_lines_deduped = [subtitle_lines[0]]
                for line_a, line_b in zip(subtitle_lines[:-1], subtitle_lines[1:]):
                    if line_a != line_b:
                        subtitle_lines_deduped.append(line_b)
                subs = '\n'.join(subtitle_lines_deduped)
            else:
                subs = None


            video_data = {k:v for k,v in video.items() if k in KEYS}
            video_data["subs"] = subs
            video_data["subtitle_lang"] = subtitle_lang
            video_data["error"] = False
            DB.query(INSERT_QUERY, **video_data)
            break
