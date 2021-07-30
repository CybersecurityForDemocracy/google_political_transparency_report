import csv
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import logging
from os import environ
from random import shuffle 
import sys

import requests
import webvtt
import youtube_dl
from dotenv import load_dotenv
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
    # "subs", # don't write subs anymore, since it's handled by youtube_video_subs
    # "subtitle_lang", # don't write subs anymore, since it's handled by youtube_video_subs
    "error",
    "video_unavailable",
    "video_private"
]
INSERT_QUERY = "INSERT INTO youtube_videos ({}) VALUES ({})  ON CONFLICT (id) DO UPDATE SET {}".format(', '.join([k for k in KEYS]), ', '.join([":" + k for k in KEYS]), ', '.join([f"{k} = :{k}" for k in KEYS]))
INSERT_SUBS_QUERY = "INSERT INTO youtube_video_subs ({}) VALUES ({})  ON CONFLICT (id) DO UPDATE SET {}".format(', '.join([k for k in ["id", "subs", "subtitle_lang", "asr"]]), ', '.join([":" + k for k in ["id", "subs", "subtitle_lang", "asr"]]), ', '.join([f"{k} = :{k}" for k in ["id", "subs", "subtitle_lang", "asr"]]))

SUBTITLE_RATE_LIMIT_STRING = "your computer or network may be sending automated queries. To protect our users, we can't process your request right now";
PROXY_ENV_VARS=["SOCKS5USERNAME", "SOCKS5PASSWORD", "SOCKS5PORT", "SOCKS5URLS"]


COUNT_TO_SCRAPE_PER_SCRAPER= 20

class RateLimitedOrBlockedException(Exception): pass

def get_database_connection(): 
    return records.Database(environ['DATABASE_URL'])


class YouTubeVideoScraperFactory():
    """
        A YouTubeVideoScraperFactory creates YouTubeVideoScrapers, one each per proxy. The Factory keeps assigns records to scrape to each Scraper and then logs the results.
    """
    def __init__(self,  ydl_arguments, count_to_scrape_per_scraper=COUNT_TO_SCRAPE_PER_SCRAPER, proxy_urls=[], scrape_locally_too=True):
        self.db = get_database_connection()
        self.ydl_arguments = ydl_arguments
        self.available_proxies = proxy_urls + ([None] if scrape_locally_too else [])
        self.count_to_scrape_per_scraper = count_to_scrape_per_scraper
        shuffle(self.available_proxies)

    def new_ads_from_political_transparency_report_site(self):
        return self.db.query("""
            select distinct youtube_ad_id 
            from google_ad_creatives 
            left outer join youtube_videos 
            on youtube_videos.id = youtube_ad_id 
            where youtube_ad_id is not null 
            and youtube_ad_id != 'simgad'
            and (youtube_videos.id is null or (youtube_videos.error = true))""")

    def new_ads_from_ad_observer(self):
        return self.db.query("""
            select distinct platformitemid as youtube_ad_id
            from observations.youtube_ads
            left outer join youtube_videos 
            on youtube_videos.id = observations.youtube_ads.platformitemid 
            where (youtube_videos.id is null or (youtube_videos.error = true)) and itemtype != 'recommendedVideo'
            """)

    def what_to_scrape(self): 
        return list(self.new_ads_from_political_transparency_report_site()) + list(self.new_ads_from_ad_observer())

    def scrape(self):
        what_to_scrape = self.what_to_scrape()
        log.info("attempted to scrape from a list of {} videos".format(len(what_to_scrape)))
        overall_results = {"total_to_scrape": len(what_to_scrape)}        
        for i, proxy in enumerate(self.available_proxies):
            scraper = YouTubeVideoScraper(self.ydl_arguments, what_to_scrape[i * self.count_to_scrape_per_scraper:(i+1) * self.count_to_scrape_per_scraper], proxy=proxy)
            res = scraper.scrape()
            duration, success_count, error_count, unavailable_count, private_count = res
            overall_results["duration"] = overall_results.get("duration", timedelta(seconds=0)) + duration
            overall_results["attempted_count"] = overall_results.get("attempted_count", 0) + success_count + error_count + unavailable_count + private_count
            overall_results["success_count"] = overall_results.get("success_count", 0) + success_count
            overall_results["error_count"] = overall_results.get("error_count", 0) + error_count
            overall_results["unavailable_count"] = overall_results.get("unavailable_count", 0) + unavailable_count
            overall_results["private_count"] = overall_results.get("private_count", 0) + private_count
            overall_results["total_remaining_at_start"] = len(what_to_scrape)
        return overall_results

class YouTubeVideoScraper:
    """ 
        A YouTubeVideoScraper knows how to scrape data from YouTube and write what it gets to the DB, returning result stats. Optionally, it has proxy-settings and a rate limit/count limit.

        It gets a list of things to scrape at init time.
    """
    def __init__(self,  ydl_arguments, ad_ids_to_scrape, proxy=None ):
        self.db = get_database_connection()

        self.ydl_arguments = ydl_arguments.copy()
        self.ydl_arguments["proxy"] = self.get_proxy(proxy)
        self.ydl = youtube_dl.YoutubeDL(self.ydl_arguments)        

        self.ad_ids_to_scrape = ad_ids_to_scrape

    def get_proxy(self, proxy_server):
        if any([environ.get(var) for var in PROXY_ENV_VARS]) and proxy_server:
            log.info("using proxy {}".format(proxy_server))
            return "socks5://{}:{}@{}:{}".format(environ["SOCKS5USERNAME"], environ["SOCKS5PASSWORD"], proxy_server, environ[ "SOCKS5PORT"])
        else:
            log.info("not using a proxy")
            return None 

    def scrape(self):
        error_count = 0
        unavailable_count = 0
        private_count = 0
        success_count = 0
        start_time = datetime.now()
        for ad in self.ad_ids_to_scrape:
            if not ad.youtube_ad_id:
                continue
            try: 
                error, unavailable, private = self.get_ad_video_info(ad.youtube_ad_id)
            except RateLimitedOrBlockedException:
                break
            if error: error_count += 1
            if unavailable: unavailable_count += 1
            if private: private_count += 1
            if not error and not unavailable and not private: success_count += 1
        duration = datetime.now() - start_time
        return duration, success_count, error_count, unavailable_count, private_count

    @staticmethod
    def parse_webvtt_subtitles_to_text(subtitle_data):
        """
        
            Return values: 
                subtitles, as a text string
                retryable_error, boolean: if we should discard this and try again later (e.g. a weird network error or rate-limiting)
                non-rettryable_error, boolean: if we shouldn't retry, e.g. because there were no subtitles

        """
        if subtitle_data and SUBTITLE_RATE_LIMIT_STRING in subtitle_data:
            log.info("subtitle_data {}".format(subtitle_data))
            return None, True, False # if we're rate-limited, it's a retryable error
        elif subtitle_data:
            subtitle_lines = [caption.text for caption in webvtt.read_buffer(StringIO(subtitle_data)) if caption.text.strip() != '']
            subtitle_lines_deduped = [subtitle_lines[0]]
            for line_a, line_b in zip(subtitle_lines[:-1], subtitle_lines[1:]):
                if line_a not in line_b:
                    subtitle_lines_deduped.append(line_b)
            subs = '\n'.join(subtitle_lines_deduped)
            return subs, False, False
        else:
            subs = None
            return subs, False, True # if there's no subtitle data, it's a non-retryable error


    def get_subtitles(self, subtitles_url, proxy=None):
        try:
            subtitle_data = requests.get(subtitles_url, stream=True, 
                proxies=dict(http=self.ydl_arguments["proxy"],
                             https=self.ydl_arguments["proxy"])).text
            subs, retryable_error, non_retryable_error = YouTubeVideoScraper.parse_webvtt_subtitles_to_text(subtitle_data)
        except requests.exceptions.ConnectionError:
            # sometimes the proxy fails?? we should just bail out in a retryable way.
            subs = None
            retryable_error = True
            non_retryable_error = False
        return subs, retryable_error, non_retryable_error


    def get_ad_video_info(self, youtube_ad_id):
        retried = False
        video = None
        while True: # this while is just to be able to retry the fetch once if there's an error.
            try:
                video = self.ydl.extract_info(
                    f'http://www.youtube.com/watch?v={youtube_ad_id}',
                    download=False # We just want to extract the info
                )
                break
            except (youtube_dl.utils.ExtractorError, youtube_dl.utils.DownloadError) as e:
                    # Videos can be unavailable for three reasons that I'm currently aware of:
                    #   1. private videos. We might eventually want to re-scrape these, but for now, we're not doing that. video_unavailable: True, video_private: True, error: False
                    #   2. unavailable videos. Presumably permanently unavailable. We don't re-scrape these. also 'The uploader has not made this video available in your country.' 'This video is not available in your country.', `This video contains content from (whoever), who has blocked it on copyright grounds.` video_unavailable: True, video_private: False, error: False
                    #   3. other errors, often rate-limiting related. video_unavailable: False, video_private: null, error: true

                if 'video unavailable' in repr(e).lower() or 'The uploader has not made this video available in your country.' in repr(e) or 'This video is not available in your country.' in repr(e) or 'copyright grounds' in repr(e):
                    video_data = {"video_unavailable": True, "video_private": False, "error": False}
                    self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                    log.info("video unavailable")
                    return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

                elif 'no conn, hlsvp, hlsManifestUrl or url_encoded_fmt_stream_map information found in video info' in repr(e) or 'Private video' in repr(e):
                    video_data = {"video_unavailable": True, "video_private": True, "error": False}
                    self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                    log.info("video unavailable, private")
                    return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

                else:
                    if retried:
                        video_data = {"error": True, "video_unavailable": False, "video_private": False}
                        self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
                        # This video is not available in your country
                        # The uploader has not made this video available in your country
                        log.warn("unknown video fetching error, will retry: " +  repr(e))
                        return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])
                    else:
                        if 'HTTP Error 429' in repr(e):
                            print('429, sleeping 2m')
                            sleep(120)
                            raise RateLimitedOrBlockedException                          
                        elif 'urlopen error [Errno 111] Connection refused' in repr(e):
                            print('connection refused')
                            raise RateLimitedOrBlockedException
                        else:
                            sleep(5)
                        print("retrying")
                        retried = True
                        continue


            # "subtitles", if present, gives non-auto subs
            # "requested_subtitles" is auto subs.
            # "automatic_captions" is also auto subs.            
            # the human-generated lacks "kind=asr" in the URL

            # https://www.youtube.com/api/timedtext?v=PEMIxDjSRTQ&asr_langs=de%2Cen%2Ces%2Cfr%2Cit%2Cja%2Cko%2Cnl%2Cpt%2Cru&caps=asr&exp=xftt&xorp=true&xoaf=5&hl=en&ip=0.0.0.0&ipbits=0&expire=1619238985&sparams=ip%2Cipbits%2Cexpire%2Cv%2Casr_langs%2Ccaps%2Cexp%2Cxorp%2Cxoaf&signature=9D727597E5502ACD8B11C54632FACE7857F050D5.939502323CED10D7FAE99555F0A0B9FEB78CC460&key=yt8&kind=asr&lang=en&tlang=en&fmt=vtt
            # https://www.youtube.com/api/timedtext?v=PEMIxDjSRTQ&asr_langs=de%2Cen%2Ces%2Cfr%2Cit%2Cja%2Cko%2Cnl%2Cpt%2Cru&caps=asr&exp=xftt&xorp=true&xoaf=5&hl=en&ip=0.0.0.0&ipbits=0&expire=1619238985&sparams=ip%2Cipbits%2Cexpire%2Cv%2Casr_langs%2Ccaps%2Cexp%2Cxorp%2Cxoaf&signature=9D727597E5502ACD8B11C54632FACE7857F050D5.939502323CED10D7FAE99555F0A0B9FEB78CC460&key=yt8&lang=en&fmt=vtt

        has_any_subs = False
        retryable_error = False
        non_retryable_error = None
        for lang in environ.get("YOUTUBE_SUBS_LANGUAGES", "en,es,de").split(","):
            if video['requested_subtitles'] and lang in video['requested_subtitles']:
                subs, subs_retryable_error, subs_non_retryable_error = self.get_subtitles(video['requested_subtitles'][lang]['url'])
                non_retryable_error = (non_retryable_error is None) and subs_non_retryable_error
                retryable_error = retryable_error or subs_retryable_error
                has_any_subs = has_any_subs or not not subs 
                subtitle_lang = lang
                asr = True
                if not subs_retryable_error and not subs_non_retryable_error:
                    self.handle_subtitle_data(youtube_ad_id, subs, subtitle_lang, asr)
            if "subtitles" in video and video["subtitles"] and lang in video["subtitles"]:
                subs, subs_retryable_error, subs_non_retryable_error = self.get_subtitles([obj for obj in video['subtitles'][lang] if obj["ext"] == "vtt"][0]['url'])
                non_retryable_error = (non_retryable_error is None) and subs_non_retryable_error
                retryable_error = retryable_error or subs_retryable_error      
                has_any_subs = has_any_subs or not not subs                                   
                subtitle_lang = lang
                asr = False
                if not subs_retryable_error and not subs_non_retryable_error:
                    self.handle_subtitle_data(youtube_ad_id, subs, subtitle_lang, asr)
        if not has_any_subs:
            subs = None
            subtitle_lang = None
            retryable_error = False
            non_retryable_error = False

        if retryable_error:
            # don't write anything to the DB.
            log.info("subtitle query was rate-limited for {}".format(youtube_ad_id))            
            return True, False, False
        elif non_retryable_error:
            video_data = {"error": True, "video_unavailable": False, "video_private": False}
            log.info("non-retryable error for {}".format(youtube_ad_id))
            self.db.query(INSERT_QUERY, **{**{k: None for k in KEYS}, **{"id": youtube_ad_id}, **video_data})
            return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])
        else:
            if has_any_subs:
                log.info("subtitles found for {}".format(youtube_ad_id))
            else:
                log.info("no subtitles found {}".format(youtube_ad_id))
            video_data = {**{k:None for k in KEYS}, **{k:v for k,v in video.items() if k in KEYS}}
            video_data["error"] = False
            video_data["video_unavailable"] = False
            video_data["video_private"] = False
            video_data["like_count"] = video_data["like_count"] or 0
            video_data["dislike_count"] = video_data["dislike_count"] or 0
            if video_data["upload_date"]:
                video_data["upload_date"] = str(video_data["upload_date"])

            try:
                self.db.query(INSERT_QUERY, **video_data)
            except ValueError as e:
                logging.error('%r trying to insert video_data: %r', e, video_data)
                raise
            return (video_data["error"], video_data["video_unavailable"], video_data["video_private"])

    def handle_subtitle_data(self, youtube_ad_id, subs, subtitle_lang, asr):
        self.db.query(INSERT_SUBS_QUERY, **{"id": youtube_ad_id, "subs": subs, "subtitle_lang": subtitle_lang, "asr": asr})

def scrape_new_ads():
    ydl_args = {
        'outtmpl': '%(id)s.%(ext)s',
        'writeautomaticsub': True,
        'subtitleslangs': ['en'],
    }

    factory = YouTubeVideoScraperFactory(ydl_args, count_to_scrape_per_scraper=COUNT_TO_SCRAPE_PER_SCRAPER, proxy_urls=environ.get("SOCKS5URLS", "").split(","), scrape_locally_too=True)

    # garbage below.

    SUCCESS_PROPORTION_WARN_THRESHOLD = 0.75 # percent
    DURATION_PER_VIDEO_WARN_THRESHOLD = 10 # seconds
    MIN_SCRAPED_ADS_TO_ALERT_ABOUT = 4 # videos (if there's only 3 ads, then them all being errors might not be a problem for us)

    results = factory.scrape()
    duration = results["duration"]
    success_count = results["success_count"]
    error_count = results["error_count"]
    unavailable_count = results["unavailable_count"]
    private_count = results["private_count"]
    total_attempted = results["attempted_count"]
    total_remaining_at_start = results["total_remaining_at_start"]

    log1 = "scraped {}/{} videos from YouTube in {}; {}s per video".format(total_attempted, total_remaining_at_start, formattimedelta(duration), duration / total_attempted if total_attempted > 0 else "NA" )
    log2 = "success: {}; error: {} (private: {}, unavailable: {})".format(success_count, error_count, private_count, unavailable_count)
    log.info(log1)
    log.info(log2)
    if total_attempted > 0 and SUCCESS_PROPORTION_WARN_THRESHOLD > ( success_count / total_attempted and total_attempted > MIN_SCRAPED_ADS_TO_ALERT_ABOUT):  
        warn_msg = "proportion of scrapable youtube ads was less than expected (expected: >= {}, got: {})".format(SUCCESS_PROPORTION_WARN_THRESHOLD * 100 , int(( success_count / total_attempted) * 100))
        log.warning(log1)
        log.warning(log2)
        warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
    elif total_attempted > 0 and DURATION_PER_VIDEO_WARN_THRESHOLD < (duration / total_attempted).total_seconds(): 
        warn_msg = "youtube video fetch time more than expected. (expected: <= {}, got: {}) ".format(DURATION_PER_VIDEO_WARN_THRESHOLD,  (duration / total_attempted).total_seconds())
        log.warning(log1)
        log.warning(log2)
        warn_to_slack("Google ads: " + log1 + '\n' + log2 + '\n' + warn_msg)
    else:
        log.info(log1)
        log.info(log2)
        info_to_slack("Google ads: " + log1 + '\n' + log2)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('USAGE: {} <env file path>')
        sys.exit(1)
    load_dotenv(sys.argv[1])
    scrape_new_ads()
