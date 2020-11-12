# Google Ad Library Data

Ads on Google come in four flavors:

 - YouTube video ads
 - text/search ads 
 - image ads (from DoubleClick, I *think*)
 - image and text ads  (from DoubleClick, I *think*)

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
- creative_stats should be kept fully up to date; temporal changes in spend/impressions estimates should be checked against archived CSVs
- do YouTube videos change? I dunno. Probably doesn't matter. Maybe the transcripts aren't instant? We'll find out!


createdb googleads
csvgrep -c Advertiser_ID -m AR105500339708362752 google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv > AR105500339708362752.csv
csvgrep -c Advertiser_ID -m AR488306308034854912 google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv > AR488306308034854912.csv
(manually lowercase the headers)
csvsql --db postgresql:///googleads --tables creative_stats --insert AR105500339708362752.csv # needs to parse out min_imps/max_imps
csvsql --db postgresql:///googleads --tables creative_stats --insert --no-create AR488306308034854912.csv # needs to parse out min_imps/max_imps
USER=toreardstogentedstallopp PASSWORD=d05011d231e3fe7e86e7d742084b2d2c8f846fb1 ruby from_couchdb.rb
csvsql --db postgresql:///googleads --tables google_ad_creatives --insert djtfp_youtube_20201105.csv

python get_ad_video_info_from_youtube.py
csvsql --db postgresql:///googleads --tables youtube_videos --insert trump_ad_video_info.csv
csvsql --db postgresql:///googleads --tables youtube_videos --insert --no-create trump_ad_video_info.csv
