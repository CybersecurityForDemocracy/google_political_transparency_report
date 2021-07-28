import os
import sys
import csv
from time import sleep
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import date, timedelta, datetime
import logging

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from dotenv import load_dotenv

#  load_dotenv()
import records

from ..common.post_to_slack import info_to_slack, warn_to_slack
from ..common.formattimedelta import formattimedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(
    "google_political_transparency_report.youtube_dot_com.get_ad_video_info"
)

#  DB = records.Database()

AD_DATA_KEYS = [
    "ad_id",
    "ad_type",
    "error",
    "policy_violation_date",
    "youtube_ad_id",
    "text",
    "image_url",
    "image_urls",
    "destination",
]


# we don't ever want to overwrite data
# so if there's a conflict, the only thing we update is whether there is now an error
INSERT_QUERY = """
  INSERT INTO google_ad_creatives 
    (advertiser_id, ad_id, ad_type, error, policy_violation_date, youtube_ad_id, ad_text, image_url, image_urls, destination) 
  VALUES (:advertiser_id, {}) 
  ON CONFLICT (ad_id) 
  DO UPDATE SET error = :error, policy_violation_date = least(:policy_violation_date, google_ad_creatives.policy_violation_date)""".format(
    ", ".join([":" + k for k in AD_DATA_KEYS])
)


def write_row_to_db(ad_data):
    DB.query(INSERT_QUERY, **ad_data)


TRANSPARENCY_REPORT_PAGE_URL_TEMPLATE = "https://transparencyreport.google.com/political-ads/advertiser/{}?campaign_creatives=start:{};end:{};spend:;impressions:;type:;sort:3&lu=campaign_creatives"
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")


def is_image_iframe_ad(ad):
    try:
        ad.find_element_by_tag_name("iframe")
        return True
    except NoSuchElementException:
        return False


def is_image_img_ad(ad):
    try:
        ad.find_element_by_tag_name("img")
        return True
    except NoSuchElementException:
        return False


def is_youtube_video_ad(ad):
    try:
        ad.find_element_by_css_selector("figure.video-preview")
        return True
    except NoSuchElementException:
        return False


def is_other_video_ad(ad):
    """some ads, like from https://transparencyreport.google.com/political-ads/advertiser/AR289367614772215808/creative/CR91503350528344064 show up as "unrenderable"
    on the search results page.
    """
    try:
        ad.find_element_by_tag_name("unrenderable-ad")
        return "Video ad" in ad.find_element_by_tag_name("figcaption").text
    except NoSuchElementException:
        return False


def is_text_ad(ad):
    try:
        ad.find_element_by_tag_name("text-ad")
        return True
    except NoSuchElementException:
        return False


def is_image_and_text_ad(driver):
    """ driver is an iframe within the ad, not the ad itself """
    try:
        driver.find_element_by_tag_name("canvas")
        return True
    except NoSuchElementException:
        return False


def is_policy_violation(ad):
    try:
        elem = ad.find_element_by_tag_name("unrenderable-ad")
        return "Policy violation" in elem.text
    except NoSuchElementException:
        return False


def is_gmail_ad(ad):
    return False


def is_still_loading(ad):
    try:
        ad.find_element_by_tag_name("mat-progress-spinner")
        return True
    except NoSuchElementException:
        return False


def remove_element(driver, element):
    driver.execute_script(
        """
  var element = arguments[0];
  element.parentNode.removeChild(element);
  """,
        element,
    )


def add_class(driver, element, newClass):
    driver.execute_script(
        """
  var element = arguments[0];
  var newClass = arguments[1];
  element.classList.add(newClass);
  """,
        element,
        newClass,
    )


def empty_element(driver, element):
    driver.execute_script(
        """
  var element = arguments[0];
  element.innerHTML = "";
  """,
        element,
    )


def scrape_political_transparency_report(advertiser_id, start_date, end_date):
    """
    scrapes to an iterator the content from the Google Political Transparency Report advertiser index pages.
    """

    while True:
        try:
            driver = webdriver.Chrome(
                ChromeDriverManager().install(), options=CHROME_OPTIONS
            )

            driver.get(
                TRANSPARENCY_REPORT_PAGE_URL_TEMPLATE.format(
                    advertiser_id,
                    int(start_date.strftime("%s")) * 1000,
                    int(end_date.strftime("%s")) * 1000,
                )
            )
            sleep(2)
            while True:
                start_time = datetime.now()
                ads = driver.find_elements_by_css_selector(
                    "creative-preview:not(.alreadyprocessed)"
                )
                log.info("got {} ads".format(len(ads)))
                if not ads:
                    sleep(10)
                    ads = driver.find_elements_by_css_selector(
                        "creative-preview:not(.alreadyprocessed)"
                    )
                    log.info("got {} ads (second attempt)".format(len(ads)))
                    if not ads:
                        break
                for i, ad in enumerate(ads):
                    ad_detail_url = ad.find_element_by_tag_name("a").get_attribute(
                        "href"
                    )
                    ad_id = ad_detail_url.split("/")[-1]
                    if i == 0:
                        log.info(
                            f"new tranche, first creative id: {ad_id}, advertiser: {advertiser_id}"
                        )
                    log.debug(f"ad_id {ad_id}")
                    log.debug(
                        driver.execute_script("return arguments[0].outerHTML;", ad)
                    )
                    if is_still_loading(ad):
                        sleep(1)
                    if is_still_loading(ad):
                        sleep(5)
                    if is_youtube_video_ad(ad):
                        try:
                            img_url = ad.find_element_by_tag_name("img").get_attribute(
                                "src"
                            )
                        except NoSuchElementException:
                            log.warning("no img?")
                            log.warning(ad, ad.get_attribute("innerHTML"))
                            yield {"ad_id": ad_id, "error": True, "ad_type": ad_type}
                        youtube_ad_id = img_url.split("/")[4]
                        ad_type = "video"
                        log.debug(f"ad type: {ad_type}")
                        yield {
                            "ad_id": ad_id,
                            "youtube_ad_id": youtube_ad_id,
                            "ad_type": ad_type,
                            "policy_violation_date": None,
                        }
                    if is_other_video_ad(ad):
                        ad_type = "video"
                        log.debug(f"ad type: {ad_type}")
                        yield {
                            "ad_id": ad_id,
                            "ad_type": ad_type,
                            "error": True,
                            "policy_violation_date": None,
                        }
                    elif is_text_ad(ad):
                        ad_container = ad.find_element_by_tag_name("text-ad")
                        remove_element(
                            driver,
                            ad_container.find_element_by_css_selector(".ad-icon"),
                        )
                        text = "\n".join(
                            [
                                div.text
                                for div in ad_container.find_elements_by_css_selector(
                                    "div"
                                )
                            ]
                        )
                        ad_type = "text"
                        log.debug(f"ad type: {ad_type}")
                        yield {"ad_id": ad_id, "text": text, "ad_type": ad_type}
                    elif is_image_img_ad(ad):
                        image_urls = None
                        image_url = ad.find_element_by_tag_name("img").get_attribute(
                            "src"
                        )
                        destination = ad.find_element_by_tag_name("a").get_attribute(
                            "href"
                        )
                        parsed_destination = parse_qs(urlparse(destination).query)
                        if (
                            "adurl" in parsed_destination
                            and len(parsed_destination["adurl"]) >= 1
                        ):
                            destination = parsed_destination["adurl"][0]

                        ad_text = None
                        ad_type = "image"
                        log.debug(f"ad type: {ad_type}")
                        yield {
                            "ad_id": ad_id,
                            "text": ad_text,
                            "error": False,
                            "image_url": image_url,
                            "image_urls": image_urls,
                            "destination": destination,
                            "ad_type": ad_type,
                            "policy_violation_date": None,
                        }
                    elif is_image_iframe_ad(ad):
                        iframe = driver.find_element_by_tag_name("iframe")
                        iframe_url = iframe.get_attribute("src")
                        driver.switch_to.frame(iframe)
                        if is_image_and_text_ad(driver):
                            # image and text ad
                            image_url = driver.find_element_by_tag_name(
                                "canvas"
                            ).value_of_css_property("background-url")
                            image_urls = None
                            try:
                                destination = driver.find_element_by_tag_name(
                                    "a"
                                ).get_attribute("href")
                                # occasionally missing, e.g. https://transparencyreport.google.com/political-ads/advertiser/AR182710451392479232/creative/CR315072959679037440
                            except NoSuchElementException:
                                destination = None
                            ad_text = driver.find_element_by_tag_name("html").text
                            ad_type = "image_and_text"
                        else:  # then it's an image ad
                            try:
                                iframe = driver.find_element_by_tag_name("iframe")
                                iframe_url = iframe.get_attribute("src")
                                driver.switch_to.frame(iframe)
                            except NoSuchElementException:
                                pass
                            image_urls = [
                                urljoin(iframe_url, img.get_attribute("src"))
                                for img in driver.find_elements_by_tag_name("img")
                            ]
                            image_url = None
                            try:
                                destination = driver.find_element_by_tag_name(
                                    "a"
                                ).get_attribute("href")
                            except NoSuchElementException as e:
                                destination = None
                                pass
                            ad_text = None
                            ad_type = "image"
                        driver.switch_to.default_content()
                        ad_type = "image"
                        log.debug(f"ad type: {ad_type}")
                        yield {
                            "ad_id": ad_id,
                            "text": ad_text,
                            "error": False,
                            "image_url": image_url,
                            "image_urls": image_urls,
                            "destination": destination,
                            "ad_type": ad_type,
                            "policy_violation_date": None,
                        }
                    elif is_policy_violation(ad):
                        ad_type = "unknown"
                        log.debug(f"ad type: {ad_type}")
                        yield {
                            "ad_id": ad_id,
                            "error": False,
                            "ad_type": ad_type,
                            "policy_violation_date": date.today(),
                        }
                    elif is_gmail_ad(ad):
                        pass
                    else:
                        # sometimes this appears to happen sporadically, like the page isn't done loading yet?
                        log.warning(
                            f"unrecognized ad type {ad_id} / advertiser: {advertiser_id}"
                        )
                        ad_type = "unknown"
                        log.debug(f"ad type: {ad_type}")
                        log.debug(
                            driver.execute_script("return arguments[0].outerHTML;", ad)
                        )
                        yield {
                            "ad_id": ad_id,
                            "error": True,
                            "ad_type": ad_type,
                            "policy_violation_date": None,
                        }
                    add_class(driver, ad, "alreadyprocessed")
                    empty_element(
                        driver, ad
                    )  # iframes and stuff take up a lot of memory. we empty out elements once we've processed them. (we empty them out, instead of removing them, because removing them causes weird behavior)
                log.info(
                    "took: {}".format((datetime.now() - start_time).total_seconds())
                )
                try:
                    load_more_btn = driver.find_element_by_tag_name(
                        "button.ng-star-inserted"
                    )
                    load_more_btn.click()
                    sleep(2)
                except NoSuchElementException:
                    break
        except WebDriverException:
            pass  # retry
        else:
            return  # we're done if we didn't get a WebDriverException


def backfill_empty_advertisers(start_date, end_date):
    """from an empty database, go get ALL ads from start_date

    excluding any advertisers for whom we have a row in google_ad_creatives for every ad in creative_stats

    TODO: write about purpose of max_report_date...
    """
    advertiser_ids = DB.query(
        """
      select creative_stats.advertiser_id, count(*) 
        from creative_stats 
        left outer join google_ad_creatives using (ad_id) 
        join (select distinct advertiser_id from advertiser_weekly_spend where week_start_date >= :start_date  and week_start_date <= :end_date ) recent_advertisers on recent_advertisers.advertiser_id = creative_stats.advertiser_id
        where date_range_start >= :start_date
        and date_range_end <= :end_date
        and google_ad_creatives.ad_id is null 
        and creative_stats.report_date = (SELECT max(report_date) FROM creative_stats)
        group by creative_stats.advertiser_id 
        order by count(*) desc;""",
        start_date=start_date,
        end_date=end_date,
    )
    for advertiser in advertiser_ids:
        advertiser_id = advertiser["advertiser_id"]
        print("starting advertiser {}".format(advertiser_id))
        with open(
            f"data/{advertiser_id}_{start_date}_{end_date}_scrape.csv", "w"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=AD_DATA_KEYS)
            writer.writeheader()
            for row in scrape_political_transparency_report(
                advertiser_id, start_date, end_date
            ):
                ad_data = {k: None for k in AD_DATA_KEYS}
                ad_data.update(row)
                writer.writerow(ad_data)
                ad_data["advertiser_id"] = advertiser_id
                write_row_to_db(ad_data)


def scrape_individual_advertiser_to_csv(advertiser_id, start_date, end_date):
    with open(
        f"data/{advertiser_id}_{start_date}_{end_date}_scrape.csv", "w"
    ) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=AD_DATA_KEYS)
        writer.writeheader()
        for row in scrape_political_transparency_report(
            advertiser_id, start_date, end_date
        ):
            writer.writerow(row)


def scrape_individual_advertiser_to_db(advertiser_id, start_date, end_date):
    for row in scrape_political_transparency_report(
        advertiser_id, start_date, end_date
    ):
        ad_data = {k: None for k in AD_DATA_KEYS}
        ad_data.update(row)
        ad_data["advertiser_id"] = advertiser_id
        write_row_to_db(ad_data)


def scrape_individual_ad(advertiser_id, creative_id):
    pass  # AR393609425983635456 CR381226726031622144


def running_update_of_all_advertisers():
    """
    on a daily basis, go and get all the ads that ran in the past couple days for each advertiser
    """

    # get all the spenders who have spent any money in the past week AND who have ads whose max(date_range_end) is no more than 2 days before the overall max(date_range_end)
    # then go get all their ads after that date
    advertisers = DB.query(
        """
    select advertiser_id, advertiser_weekly_spend.advertiser_name, date_range_end_max - interval '1 day' one_days_before_max_ad_date from 
      (select advertiser_id, max(date_range_end) date_range_end_max 
      from creative_stats 
      group by advertiser_id) advertisers_this_week
    join advertiser_weekly_spend
      using (advertiser_id)
    where advertiser_weekly_spend.week_start_date = (select max(week_start_date) from advertiser_weekly_spend)
    order by spend_usd desc
  """
    )
    end_date = date.today()  # date(2020, 9, 1)
    ad_count = 0
    unrecognized_ad_count = 0
    start_time = datetime.now()
    for advertiser in advertisers:
        advertiser_id = advertiser["advertiser_id"]
        log.info(
            "starting advertiser {} - {}".format(
                advertiser["advertiser_name"], advertiser_id
            )
        )
        start_date = advertiser["one_days_before_max_ad_date"]
        for row in scrape_political_transparency_report(
            advertiser_id, start_date, end_date
        ):
            ad_data = {k: None for k in AD_DATA_KEYS}
            ad_data.update(row)
            ad_data["advertiser_id"] = advertiser_id
            write_row_to_db(ad_data)
            ad_count += 1
            if ad_data["error"] and ad_data["ad_type"] == "unknown":
                unrecognized_ad_count += 1
    duration = datetime.now() - start_time

    AD_COUNT_WARN_THRESHOLD = 50
    ADVERTISER_COUNT_WARN_THRESHOLD = 10
    PER_AD_DURATION_WARN_THRESHOLD = 3  # seconds
    UNRECOGNIZED_AD_TYPE_COUNT_WARN_THRESHOLD = 0.1  # proportion
    log_msg = "scraped {} ads from transparency report site from {} advertisers in {} ({} / advertiser, {}/ ad). {} ads of unrecognized type.".format(
        ad_count,
        len(advertisers),
        formattimedelta(duration),
        formattimedelta(duration / len(advertisers)),
        formattimedelta(duration / ad_count),
        unrecognized_ad_count,
    )
    if AD_COUNT_WARN_THRESHOLD > ad_count:
        warn_msg = "political transparency report site scraper found fewer ads than expected (expected: {}, got: {})".format(
            AD_COUNT_WARN_THRESHOLD, ad_count
        )
        log.warn(log_msg)
        log.warn(warn_msg)
        warn_to_slack("Google ads: " + log_msg + "\n" + warn_msg)
    elif ADVERTISER_COUNT_WARN_THRESHOLD > len(advertisers):
        warn_msg = "political transparency report site scraper found fewer advertisers than expected (expected: {}, got: {})".format(
            ADVERTISER_COUNT_WARN_THRESHOLD, len(advertisers)
        )
        log.warn(log_msg)
        log.warn(warn_msg)
        warn_to_slack("Google ads: " + log_msg + "\n" + warn_msg)
    elif PER_AD_DURATION_WARN_THRESHOLD < (duration / ad_count).total_seconds():
        warn_msg = "political transparency report site scraper took longer than expected to scrape each ad (expected: {}, got: {})".format(
            PER_AD_DURATION_WARN_THRESHOLD, (duration / ad_count).total_seconds()
        )
        log.warn(log_msg)
        log.warn(warn_msg)
        warn_to_slack("Google ads: " + log_msg + "\n" + warn_msg)
    elif UNRECOGNIZED_AD_TYPE_COUNT_WARN_THRESHOLD < (unrecognized_ad_count / ad_count):
        warn_msg = "political transparency report site scraper found a greater proportion of ads of unknown type (expected: < {}, got: {})".format(
            UNRECOGNIZED_AD_TYPE_COUNT_WARN_THRESHOLD,
            (unrecognized_ad_count / ad_count),
        )
        log.warn(log_msg)
        log.warn(warn_msg)
        warn_to_slack("Google ads: " + log_msg + "\n" + warn_msg)
    else:
        log.info(log_msg)
        info_to_slack("Google ads: " + log_msg)


def main():
    scrape_one_advertiser_to_db = os.environ.get("SCRAPE_ONE_ADVERTISER_TO_DB", False)
    scrape_one_advertiser_to_csv = os.environ.get("SCRAPE_ONE_ADVERTISER_TO_CSV", False)
    backfill_empty_advertisers = os.environ.get("BACKFILL_EMPTY_ADVERTISERS", False)
    if scrape_one_advertiser_to_csv:
        advertiser_id = scrape_one_advertiser_to_csv  # TMAGAC: AR488306308034854912 ; DJT4P: AR105500339708362752
        start_date = date(2020, 1, 1)
        end_date = date.today()  # date(2020, 9, 1)
        scrape_individual_advertiser_to_csv(advertiser_id, start_date, end_date)
    if scrape_one_advertiser_to_db:
        advertiser_id = scrape_one_advertiser_to_db  # TMAGAC: AR488306308034854912 ; DJT4P: AR105500339708362752
        start_date = date(2020, 1, 1)
        end_date = date.today()
        scrape_individual_advertiser_to_db(advertiser_id, start_date, end_date)
    elif backfill_empty_advertisers:
        log.info("backfilling empty advertisers")
        # start_date = date(2020, 5, 1)
        # end_date = date(2020, 9, 2)
        # start_date = date(2020, 9, 1)
        # end_date = date(2020, 10, 1)
        start_date = date(2020, 1, 1)
        end_date = date.today()
        backfill_empty_advertisers(start_date, end_date)
    else:
        # on a daily basis
        running_update_of_all_advertisers()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('USAGE: {} <env file path>')
        sys.exit(1)
    load_dotenv(sys.argv[1])
    # TODO(macpd): move this out of global scope
    DB = records.Database()
    main()
