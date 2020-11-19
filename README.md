# Google Ad Library Data

Ads on Google come in four flavors that Jeremy can detect on Google's political transparency report site:

 - YouTube video ads
 - text/search ads 
 - image ads (from DoubleClick, I *think*)
 - image and text ads  (from DoubleClick, I *think*)

Plus:

 - "policy violation" ads (which if we never observed them before they were declared to violate Google policies, we don't know which flavor they belong to).

## Data sources
There are three data sources for these ads:

 - Google's downloadable "political ads transparency bundle" which contains `creative_stats.csv` which contains Ad IDs and metadata (spending, impressions, durations)
 - a scrape of Google's Poliical Ads Transparency Report website, which contains ad creatives (text, image URLs, )
 - a scrape of YouTube for each unique YouTube video that is an ad, to get its transcript, title, view count, etc.

This data joins up via YouTube video IDs to Ad Observer observations, which are otherwise outside the scope of this document.

## Database stuff
Each of those data sources has its own table in Postgres.

- creative_stats (creative_stats.csv)
- google_ad_creatives (scraped Transparency Report website data)
- youtube_videos (youtube-scraped video data, transcripts etc.)

## Temporal aspects

- we should scrape the Transparency Report website data frequently, since that data can disappear.
- creative_stats and advertiser_weekly_spend are synced to the latest copy; each day's copy of advertiser_stats is kept (so you can chart total spend and total ads per day).
- do YouTube videos change? I dunno. Probably doesn't matter. Maybe the transcripts aren't instant? We'll find out!


## How to deploy this

I haven't figured out how, exactly, since I need to figure out which database it will live on.

SQL tables were created manually.