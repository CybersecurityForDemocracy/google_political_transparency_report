
import csv

import requests
import webvtt
import youtube_dl
from dotenv import load_dotenv
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import logging

load_dotenv()
import records

from ..common.post_to_slack import info_to_slack, warn_to_slack
from ..common.formattimedelta import formattimedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("google_political_transparency_report.youtube_dot_com.get_ad_video_info")


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
    "video_unavailable",
    "video_private"
]
INSERT_QUERY = "INSERT INTO youtube_videos ({}) VALUES ({})  ON CONFLICT (id) DO UPDATE SET {}".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]), ', '.join([f"{k} = :{k}" for k in KEYS]))

class YouTubeVideoScraper:
    def __init__(self, db, ydl):
        self.db = db
        self.ydl = ydl

    def get_ad_video_info(self, youtube_ad_id):
        retried = False
        while True:
            try:
                video = self.ydl.extract_info(
                    f'http://www.youtube.com/watch?v={youtube_ad_id}',
                    download=False # We just want to extract the info
                )
            except (youtube_dl.utils.ExtractorError, youtube_dl.utils.DownloadError) as e:
                    # Videos can be unavailable for three reasons that I'm currently aware of:
                    #   1. private videos. We might eventually want to re-scrape these, but for now, we're not doing that. video_unavailable: True, video_private: True, error: False
                    #   2. unavailable videos. Presumably permanently unavailable. We don't re-scrape these. also 'The uploader has not made this video available in your country.' 'This video is not available in your country.' video_unavailable: True, video_private: False, error: False
                    #   3. other errors, often rate-limiting related. video_unavailable: False, video_private: null, error: true

                if 'Video unavailable' in repr(e) or 'The uploader has not made this video available in your country.' in repr(e) or 'This video is not available in your country.' in repr(e):
                    video_data = {"video_unavailable": True, "video_private": False, "error": False}
                    self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                    print("video unavailable")
                    return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

                elif 'no conn, hlsvp, hlsManifestUrl or url_encoded_fmt_stream_map information found in video info' in repr(e):
                    video_data = {"video_unavailable": True, "video_private": True, "error": False}
                    self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                    print("video unavailable, private")
                    return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

                else:
                    if retried:
                        video_data = {"error": True, "video_unavailable": False, "video_private": False}
                        self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                        # This video is not available in your country
                        # The uploader has not made this video available in your country
                        return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

                    else:
                        if 'HTTP Error 429' in repr(e):
                            print('429, sleeping 2m')
                            sleep(120)
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
                subtitle_lines = [caption.text for caption in webvtt.read_buffer(StringIO(subtitle_data)) if caption.text.strip() != '']
                subtitle_lines_deduped = [subtitle_lines[0]]
                for line_a, line_b in zip(subtitle_lines[:-1], subtitle_lines[1:]):
                    if line_a not in line_b:
                        subtitle_lines_deduped.append(line_b)
                subs = '\n'.join(subtitle_lines_deduped)
            else:
                subs = None


            video_data = {**{k:None for k in KEYS}, **{k:v for k,v in video.items() if k in KEYS}}
            video_data["subs"] = subs
            video_data["subtitle_lang"] = subtitle_lang
            video_data["error"] = False
            video_data["video_unavailable"] = False
            video_data["video_private"] = False
            video_data["like_count"] = video_data["like_count"] or 0
            video_data["dislike_count"] = video_data["dislike_count"] or 0
            if video_data["upload_date"]
                video_data["upload_date"] = str(video_data["upload_date"])

            self.db.query(INSERT_QUERY, **video_data)
            return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])
            break


    def new_ads_from_political_transparency_report_site(self):
        return self.db.query("""
            select distinct youtube_ad_id 
            from google_ad_creatives 
            left outer join youtube_videos 
            on youtube_videos.id = youtube_ad_id 
            where youtube_ad_id is not null 
            and (youtube_videos.id is null or youtube_videos.error = true)""")

    def new_ads_from_ad_observer(self):
        return self.db.query("""
            select distinct platformitemid as youtube_ad_id
            from observed_youtube_ads
            left outer join youtube_videos 
            on youtube_videos.id = observed_youtube_ads.platformitemid 
            where (youtube_videos.id is null or youtube_videos.error = true) and itemtype != 'recommendedVideo'
            """)

    def scrape_from_list(self, ad_ids_to_scrape):
        error_count = 0
        unavailable_count = 0
        private_count = 0
        start_time = datetime.now()
        for ad in ad_ids_to_scrape:
            if not ad.youtube_ad_id:
                continue
            error, unavailable, private = self.get_ad_video_info(ad.youtube_ad_id)
            if error: error_count += 1
            if unavailable: unavailable_count += 1
            if private: private_count += 1
        duration = datetime.now() - start_time
        return duration, len(ad_ids_to_scrape) - error_count, error_count, unavailable_count, private_count

def scrape_new_ads():
    ydl = youtube_dl.YoutubeDL({
        'outtmpl': '%(id)s.%(ext)s',
        'writeautomaticsub': True,
        'subtitleslangs': ['en'],
    })
    DB = records.Database()

    with ydl:
        ytscraper = YouTubeVideoScraper(DB, ydl)
        SUCCESS_PROPORTION_WARN_THRESHOLD = 0.75 # percent
        DURATION_PER_VIDEO_WARN_THRESHOLD = 10 # seconds
        MIN_SCRAPED_ADS_TO_ALERT_ABOUT = 4 # videos (if there's only 3 ads, then them all being errors might not be a problem for us)

        scraped_youtube_video_ads = ytscraper.new_ads_from_political_transparency_report_site()
        log.info("scraping {} videos (source: transparency site) from YouTube".format(len(scraped_youtube_video_ads)))
        duration, success_count, error_count, unavailable_count, private_count = ytscraper.scrape_from_list(scraped_youtube_video_ads)
        log1 = "scraped {} videos (source: transparency site) from YouTube in {}; {}s per video".format(len(scraped_youtube_video_ads), formattimedelta(duration), duration / len(scraped_youtube_video_ads))
        log2 = "success: {}; error: {} (private: {}, unavailable: {})".format(success_count, error_count, private_count, unavailable_count)
        log.info(log1)
        log.info(log2)
        if SUCCESS_PROPORTION_WARN_THRESHOLD > ( success_count / len(scraped_youtube_video_ads) and len(scraped_youtube_video_ads) > MIN_SCRAPED_ADS_TO_ALERT_ABOUT):  
            warn_msg = "proportion of scrapable youtube ads was less than expected (expected: >= {}, got: {})".format(SUCCESS_PROPORTION_WARN_THRESHOLD * 100 , int(( success_count / len(scraped_youtube_video_ads)) * 100))
            log.warn(log1)
            log.warn(log2)
            warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
        elif DURATION_PER_VIDEO_WARN_THRESHOLD < (duration / len(scraped_youtube_video_ads)).total_seconds(): 
            warn_msg = "youtube video fetch time more than expected. (expected: <= {}, got: {}) ".format(DURATION_PER_VIDEO_WARN_THRESHOLD,  (duration / len(scraped_youtube_video_ads)).total_seconds())
            log.warn(log1)
            log.warn(log2)
            warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
        else:
            log.info(log1)
            log.info(log2)
            info_to_slack("Google ads: " + log1 + '\n' + log2)


        OBSERVED_VIDEO_WARN_THRESHOLD = 200 # count
        observed_youtube_video_ads = ytscraper.new_ads_from_ad_observer()
        log.info("scraping {} videos (source: observations) from YouTube".format(len(observed_youtube_video_ads)))
        duration, success_count, error_count, unavailable_count, private_count = ytscraper.scrape_from_list(observed_youtube_video_ads)
        log1 = "scraped {} videos (source: observations) from YouTube in {}; {}s per video".format(len(observed_youtube_video_ads), formattimedelta(duration), duration / len(observed_youtube_video_ads))
        log2 = "success: {}; error: {} (private: {}, unavailable: {})".format(success_count, error_count, private_count, unavailable_count)

        # note that we don't warn if there are no new videos in the transparency portal, just since new political ads seems rare enough that that might happen in real life.
        if OBSERVED_VIDEO_WARN_THRESHOLD > len(observed_youtube_video_ads):
            warn_msg = "number of ad observer-observed video ads is less than expected (expected: {}, got: {})".format(OBSERVED_VIDEO_WARN_THRESHOLD, len(observed_youtube_video_ads))
            log.warn(log1)
            log.warn(log2)
            log.warn(warn_msg)
            warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
        elif SUCCESS_PROPORTION_WARN_THRESHOLD > ( success_count / len(observed_youtube_video_ads)): 
            warn_msg = "proportion of scrapable youtube ads was less than expected (expected: >= {}, got: {})".format(SUCCESS_PROPORTION_WARN_THRESHOLD * 100 , int(( success_count / len(observed_youtube_video_ads)) * 100))
            log.warn(log1)
            log.warn(log2)
            warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
        elif DURATION_PER_VIDEO_WARN_THRESHOLD < (duration / len(observed_youtube_video_ads)).total_seconds(): 
            warn_msg = "youtube video fetch time more than expected. (expected: <= {}, got: {}) ".format(DURATION_PER_VIDEO_WARN_THRESHOLD, (duration / len(observed_youtube_video_ads)).total_seconds())
            log.warn(log1)
            log.warn(log2)

            warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
        else:
            log.info(log1)
            log.info(log2)
            info_to_slack("Google ads: " + log1 + '\n' + log2 + '\n')
if __name__ == "__main__":
    scrape_new_ads()