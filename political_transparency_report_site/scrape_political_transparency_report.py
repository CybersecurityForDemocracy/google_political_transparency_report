import os  
import csv
from time import sleep
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import date, timedelta, datetime

from selenium import webdriver  
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options  
from dotenv import load_dotenv

load_dotenv()
import records

DEBUG = False
DB = records.Database()

AD_DATA_KEYS = [
  "creative_id",
  "ad_type",
  "error",
  "policy_violation_date",
  "youtube_ad_id",
  "text",
  "image_url",
  "image_urls",
  "destination"
]


# we don't ever want to overwrite data
# so if there's a conflict, the only thing we update is whether there is now an error
INSERT_QUERY = """
  INSERT INTO google_ad_creatives 
    (advertiser_id, creative_id, ad_type, error, policy_violation_date, youtube_ad_id, ad_text, image_url, image_urls, destination) 
  VALUES (:advertiser_id, {}) 
  ON CONFLICT (creative_id) 
  DO UPDATE SET error = :error, policy_violation_date = least(:policy_violation_date, google_ad_creatives.policy_violation_date)""".format( ', '.join([":" + k for k in AD_DATA_KEYS]))
def write_row_to_db(ad_data):
  DB.query(INSERT_QUERY, **ad_data);


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
def is_video_ad(ad):
  try:
    ad.find_element_by_css_selector("figure.video-preview")
    return True
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
    if "Policy violation" in elem.text:
      return True
    else: 
      return False
  except NoSuchElementException:
    return False

def remove_element(driver, element):
  driver.execute_script("""
  var element = arguments[0];
  element.parentNode.removeChild(element);
  """, element)

def add_class(driver, element, newClass):
  driver.execute_script("""
  var element = arguments[0];
  var newClass = arguments[1];
  element.classList.add(newClass);
  """, element, newClass)

def empty_element(driver, element):
  driver.execute_script("""
  var element = arguments[0];
  element.innerHTML = "";
  """, element)

def scrape_political_transparency_report(advertiser_id, start_date, end_date):
  """
    scrapes to an iterator the content from the Google Political Transparency Report advertiser index pages.
  """
  driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=CHROME_OPTIONS)

  driver.get(TRANSPARENCY_REPORT_PAGE_URL_TEMPLATE.format(advertiser_id,int(start_date.strftime("%s")) * 1000, int(end_date.strftime("%s")) * 1000))
  sleep(1)

  while True:
    start_time = datetime.now()
    ads = driver.find_elements_by_css_selector("creative-preview:not(.alreadyprocessed)")
    print("got {} ads".format(len(ads)))
    if not ads:
      sleep(10)
      ads = driver.find_elements_by_css_selector("creative-preview:not(.alreadyprocessed)")
      print("got {} ads (second attempt)".format(len(ads)))
      if not ads:
        break
    for i,ad in enumerate(ads):
      ad_detail_url = ad.find_element_by_tag_name("a").get_attribute("href")
      creative_id = ad_detail_url.split("/")[-1]
      if i == 0:
        print(f"new tranche, first creative id: {creative_id}, advertiser: {advertiser_id}")
      if DEBUG: print(f"creative_id {creative_id}")
      if is_video_ad(ad):
        try:
          img_url = ad.find_element_by_tag_name("img").get_attribute("src")
        except NoSuchElementException:
          print("no img?")
          print(ad, ad.get_attribute('innerHTML'))
          continue
        youtube_ad_id = img_url.split("/")[4]
        yield {"creative_id": creative_id, "youtube_ad_id": youtube_ad_id, "ad_type": "video", "policy_violation_date": None}
      elif is_text_ad(ad):
        ad_container = ad.find_element_by_tag_name("text-ad")
        remove_element(driver, ad_container.find_element_by_css_selector(".ad-icon"))
        text = '\n'.join([div.text for div in ad_container.find_elements_by_css_selector("div")])
        yield {"creative_id": creative_id, "text": text, "ad_type": "text"}
      elif is_image_img_ad(ad):
        image_urls = None
        image_url = ad.find_element_by_tag_name("img").get_attribute("src")
        destination = ad.find_element_by_tag_name("a").get_attribute("href")
        parsed_destination = parse_qs(urlparse(url).query)
        if "adurl" in parsed_destination and len(parsed_destination["adurl"]) >= 1:
          destination = parsed_destination["adurl"][0]
        
        ad_text = None
        ad_type = "image"
        yield {"creative_id": creative_id, "text": ad_text, "error": False, "image_url": image_url,"image_urls": image_urls, "destination": destination, "ad_type": "image", "policy_violation_date": None}
      elif is_image_iframe_ad(ad):
        iframe = driver.find_element_by_tag_name("iframe")
        iframe_url = iframe.get_attribute("src")
        driver.switch_to.frame(iframe)
        if is_image_and_text_ad(driver):
          # image and text ad
          image_url = driver.find_element_by_tag_name("canvas").value_of_css_property("background-url")
          image_urls = None
          try:
            destination = driver.find_element_by_tag_name("a").get_attribute("href")
            # occasionally missing, e.g. https://transparencyreport.google.com/political-ads/advertiser/AR182710451392479232/creative/CR315072959679037440
          except NoSuchElementException:
            destination = None
          ad_text   = driver.find_element_by_tag_name("html").text
          ad_type = "image_and_text"
        else: # then it's an image ad
          try:
            iframe = driver.find_element_by_tag_name("iframe")
            iframe_url = iframe.get_attribute("src")
            driver.switch_to.frame(iframe)
          except NoSuchElementException:
            pass
          image_urls = [urljoin(iframe_url,img.get_attribute("src")) for img in driver.find_elements_by_tag_name("img")]
          image_url = None
          try:
            destination = driver.find_element_by_tag_name("a").get_attribute("href")
          except NoSuchElementException as e:
            destination = None
            pass
          ad_text = None
          ad_type = "image"
        driver.switch_to.default_content()
        yield {"creative_id": creative_id, "text": ad_text, "error": False, "image_url": image_url,"image_urls": image_urls, "destination": destination, "ad_type": "image", "policy_violation_date": None}
      elif is_policy_violation(ad):
        yield {"creative_id": creative_id, "error": False, "ad_type": "unknown", "policy_violation_date": date.today() }
      else:
        print(f"unrecognized ad type {creative_id}")
        yield {"creative_id": creative_id, "error": True, "ad_type": "unknown", "policy_violation_date": None}
      add_class(driver, ad, "alreadyprocessed")
      empty_element(driver, ad) # iframes and stuff take up a lot of memory. we empty out elements once we've processed them. (we empty them out, instead of removing them, because removing them causes weird behavior)
    print("took: {}".format((datetime.now() - start_time).total_seconds()))
    try:
      load_more_btn = driver.find_element_by_tag_name("button.ng-star-inserted")
      load_more_btn.click()
      sleep(2)
    except NoSuchElementException:
      break


def backfill_empty_advertisers(start_date, end_date):
  """ from an empty database, go get ALL ads from start_date 

      excluding any advertisers for whom we have a row in google_ad_creatives for every ad in creative_stats
  """

  # get advertisers active between start date and end date 
  # advertiser_ids = DB.query("""
  #   select advertiser_id 
  #   from advertiser_weekly_spend 
  #   where week_start_date > :start_date 
  #   and week_start_date <= :end_date 
  #   group by advertiser_id 
  #   order by sum(spend_usd) desc""", **{'start_date': start_date, 'end_date': end_date})

  advertiser_ids = DB.query("""
      select creative_stats.advertiser_id, count(*) 
        from creative_stats 
        left outer join google_ad_creatives on ad_id = creative_id 
        join (select distinct advertiser_id from advertiser_weekly_spend where week_start_date > :start_date  and week_start_date <= :end_date ) recent_advertisers on recent_advertisers.advertiser_id = creative_stats.advertiser_id
        where date_range_start > :start_date
        and google_ad_creatives.creative_id is null 
        group by creative_stats.advertiser_id 
        order by count(*) desc;""", start_date=start_date, end_date=end_date)

  for advertiser in advertiser_ids:
    advertiser_id = advertiser["advertiser_id"]
    print("starting advertiser {}".format(advertiser_id))
    with open(f'data/{advertiser_id}_{start_date}_{end_date}_scrape.csv', 'w') as csvfile:
      writer = csv.DictWriter(csvfile, fieldnames=AD_DATA_KEYS)
      writer.writeheader()
      for row in scrape_political_transparency_report(advertiser_id, start_date, end_date):
        ad_data = {k:None for k in AD_DATA_KEYS}
        ad_data.update(row)
        writer.writerow(ad_data)
        ad_data["advertiser_id"] = advertiser_id
        write_row_to_db(ad_data)

def scrape_individual_advertiser_to_csv(advertiser_id, start_date, end_date): 
  with open(f'data/{advertiser_id}_{start_date}_{end_date}_scrape.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=AD_DATA_KEYS)
    writer.writeheader()
    for row in scrape_political_transparency_report(advertiser_id, start_date, end_date):
      writer.writerow(row)

def scrape_individual_advertiser_to_db(advertiser_id, start_date, end_date): 
  for row in scrape_political_transparency_report(advertiser_id, start_date, end_date):
    ad_data = {k:None for k in AD_DATA_KEYS}
    ad_data.update(row)
    ad_data["advertiser_id"] = advertiser_id
    write_row_to_db(ad_data)


def running_update_of_all_advertisers():
  """ 
    on a daily basis, go and get all the ads that ran in the past couple days for each advertiser
  """
  
  # get all the spenders who have spent any money in the past week AND who have ads whose max(date_range_end) is no more than 2 days before the overall max(date_range_end)
  # then go get all their ads after that date
  advertisers = DB.query("""
    select advertiser_id, advertisers_this_week.advertiser_name, date_range_end_max - interval '1 day' one_days_before_max_ad_date from 
      (select advertiser_id, creative_stats.advertiser_name, max(date_range_end) date_range_end_max 
      from creative_stats 
      group by advertiser_id, creative_stats.advertiser_name) advertisers_this_week
    join advertiser_weekly_spend
      using (advertiser_id)
    where advertiser_weekly_spend.week_start_date = '2020-11-01' -- (select max(week_start_date) from advertiser_weekly_spend)
    order by spend_usd desc
    limit 100
  """)
  end_date   = date.today() #date(2020, 9, 1)
  for advertiser in advertisers:
    advertiser_id = advertiser["advertiser_id"]
    print("starting advertiser {} - {}".format(advertiser["advertiser_name"], advertiser_id))
    start_date = advertiser["one_days_before_max_ad_date"]
    with open(f'data/{advertiser_id}_{start_date}_{end_date}_scrape.csv', 'w') as csvfile:
      for row in scrape_political_transparency_report(advertiser_id, start_date, end_date):
        ad_data = {k:None for k in AD_DATA_KEYS}
        ad_data.update(row)
        ad_data["advertiser_id"] = advertiser_id
        write_row_to_db(ad_data)

SCRAPE_ONE_ADVERTISER_TO_DB = os.environ.get("SCRAPE_ONE_ADVERTISER_TO_DB", False)
SCRAPE_ONE_ADVERTISER_TO_CSV = os.environ.get("SCRAPE_ONE_ADVERTISER_TO_CSV", False)
BACKFILL_EMPTY_ADVERTISERS = os.environ.get("BACKFILL_EMPTY_ADVERTISERS", False)
if __name__ == "__main__":
  if SCRAPE_ONE_ADVERTISER_TO_CSV:
    advertiser_id = SCRAPE_ONE_ADVERTISER_TO_CSV      # TMAGAC: AR488306308034854912 ; DJT4P: AR105500339708362752
    start_date = date(2020, 6, 1)
    end_date   = date.today() #date(2020, 9, 1)
    scrape_individual_advertiser_to_csv(advertiser_id, start_date, end_date)
  if SCRAPE_ONE_ADVERTISER_TO_DB:
    advertiser_id = SCRAPE_ONE_ADVERTISER_TO_DB      # TMAGAC: AR488306308034854912 ; DJT4P: AR105500339708362752
    start_date = date(2020, 6, 1)
    end_date   = date.today() #date(2020, 9, 1)
    scrape_individual_advertiser_to_db(advertiser_id, start_date, end_date)
  elif BACKFILL_EMPTY_ADVERTISERS:
    print("backfilling empty advertisers")
    start_date = date(2020, 10, 1)
    end_date   = date(2020, 11, 4)
    # start_date = date(2020, 11, 5)
    # end_date   = date.today() #date(2020, 9, 1)
    backfill_empty_advertisers(start_date, end_date)
  else:
    # on a daily basis
    running_update_of_all_advertisers()

