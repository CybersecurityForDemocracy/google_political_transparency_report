import os  
from selenium import webdriver  
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options  
import csv
from time import sleep
from urllib.parse import urljoin
from datetime import date, timedelta, datetime

chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver", chrome_options=chrome_options)
advertiser_id = "AR99922379781701632" # TMAGAC: AR488306308034854912 ; DJT4P: AR105500339708362752
# start_date = date(2020, 9, 1)
# end_date   = date.today()
start_date = date(2019, 1, 1)
end_date   = date.today() #date(2020, 9, 1)

driver.get("https://transparencyreport.google.com/political-ads/advertiser/{}?campaign_creatives=start:{};end:{};spend:;impressions:;type:;sort:3&lu=campaign_creatives".format(advertiser_id,int(start_date.strftime("%s")) * 1000, int(end_date.strftime("%s")) * 1000))
sleep(1)

def is_image_ad(ad):
  try:
    ad.find_element_by_tag_name("iframe")
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
  try:
    driver.find_element_by_tag_name("canvas")
    return True
  except NoSuchElementException:
    return False

def remove_element(element):
  driver.execute_script("""
  var element = arguments[0];
  element.parentNode.removeChild(element);
  """, element)

def add_class(element, newClass):
  driver.execute_script("""
  var element = arguments[0];
  var newClass = arguments[1];
  element.classList.add(newClass);
  """, element, newClass)

def empty_element(element):
  driver.execute_script("""
  var element = arguments[0];
  element.innerHTML = "";
  """, element)

KEYS = [
  "creative_id",
  "ad_type",
  "error",
  "youtube_ad_id",
  "text",
  "image_url",
  "image_urls",
  "destination"
]
with open(f'data/{advertiser_id}_{start_date}_{end_date}_scrape.csv', 'w') as csvfile:
  writer = csv.DictWriter(csvfile, fieldnames=KEYS)
  writer.writeheader()

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
        print(f"new tranche, first creative id: {creative_id}")
      print(f"creative_id {creative_id}")
      if is_video_ad(ad):
        try:
          img_url = ad.find_element_by_tag_name("img").get_attribute("src")
        except NoSuchElementException:
          print("no img?")
          print(ad, ad.get_attribute('innerHTML'))
          continue
        youtube_ad_id = img_url.split("/")[4]
        writer.writerow({"creative_id": creative_id, "youtube_ad_id": youtube_ad_id, "ad_type": "video"})
      elif is_text_ad(ad):

        ad_container = ad.find_element_by_tag_name("text-ad")
        remove_element(ad_container.find_element_by_css_selector(".ad-icon"))
        text = '\n'.join([div.text for div in ad_container.find_elements_by_css_selector("div")])
        writer.writerow({"creative_id": creative_id, "text": text, "ad_type": "text"})
      elif is_image_ad(ad):
        iframe = driver.find_element_by_tag_name("iframe")
        iframe_url = iframe.get_attribute("src")
        driver.switch_to.frame(iframe)
        error = False
        if is_image_and_text_ad(driver):
          # image and text ad
          image_url = driver.find_element_by_tag_name("canvas").value_of_css_property("background-url")
          image_urls = None
          destination = driver.find_element_by_tag_name("a").get_attribute("href")
          ad_text   = driver.find_element_by_tag_name("html").text
          ad_type = "image_and_text"
        else:
          try:
            iframe = driver.find_element_by_tag_name("iframe")
            iframe_url = iframe.get_attribute("src")
            driver.switch_to.frame(iframe)
          except NoSuchElementException:
            pass
          image_urls = [urljoin(iframe_url,img.get_attribute("src")) for img in driver.find_elements_by_tag_name("img")]
          image_url = None
          destination = driver.find_element_by_tag_name("a").get_attribute("href")
          ad_text = None
          ad_type = "image"
        driver.switch_to.default_content()
        writer.writerow({"creative_id": creative_id, "text": ad_text, "error": error, "image_url": image_url,"image_urls": image_urls, "destination": destination, "ad_type": "image"})
      else:
        print(f"unrecognized ad type {creative_id}")
        writer.writerow({"creative_id": creative_id, "error": True, "ad_type": "unknown"})
      add_class(ad, "alreadyprocessed")
      empty_element(ad)
    print("took: {}".format((datetime.now() - start_time).total_seconds()))
    try:
      load_more_btn = driver.find_element_by_tag_name("button.ng-star-inserted")
      load_more_btn.click()
      sleep(2)
    except NoSuchElementException:
      break