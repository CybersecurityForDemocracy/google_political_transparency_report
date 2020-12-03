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

## Outstanding Problems:

Some ads appear in the creative-stats sheet some days, then later disappear. Presumably ads that were taken down for being political, then put back up on appeal.

e.g.

```
select creative_stats.advertiser_id, creative_stats.advertiser_name, count(*) creative_stats_ads_without_creative, array_agg(creative_stats.ad_id)
        from creative_stats 
        left outer join google_ad_creatives on ad_id = creative_id 
        join (select distinct advertiser_id from advertiser_weekly_spend where week_start_date > '2020-11-15' and week_start_date <= '2020-12-01') recent_advertisers  on recent_advertisers.advertiser_id = creative_stats.advertiser_id
        where date_range_start > '2020-11-15'
        and google_ad_creatives.creative_id is null 
        group by creative_stats.advertiser_id, creative_stats.advertiser_name
        order by count(*) desc;
```
```
    advertiser_id     |    advertiser_name     | creative_stats_ads_without_creative |                                                            array_agg                                                            
----------------------+------------------------+-------------------------------------+---------------------------------------------------------------------------------------------------------------------------------
 AR27609218009792512  | OZY MEDIA, INC.        |                                   6 | {CR336259174356746240,CR551992151819419648,CR345676491448647680,CR462453422411481088,CR335886439914930176,CR459770614039707648}
 AR478907476482195456 | TURNING POINT USA, NFP |                                   3 | {CR227333855927861248,CR173009185422704640,CR183283022072643584}
 AR120847323008860160 | CONSERVATIVE BUZZ LLC  |                                   1 | {CR441281226507026432}
 AR24814465610416128  | BEACHSIDE MEDIA INC    |                                   1 | {CR214785129720053760}
```


## How to deploy this

`rsync -av -e ssh --exclude='*.env' --exclude='data' --exclude='examples' . ccs1:/home/jmerrill/google_political_transparency_report`

SQL tables were created manually.

## Searching

search all ad video texts

```
    select 
        distinct youtube_videos.id, uploader, youtube_videos.title, alt_title, fulltitle, creative_stats.advertiser_name 
    FROM youtube_videos
    LEFT OUTER JOIN google_ad_creatives ON youtube_videos.id = google_ad_creatives.youtube_ad_id 
    LEFT OUTER JOIN creative_stats on ad_id = creative_id
    where setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, youtube_videos.title), 'A') || setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, subs), 'B') @@ plainto_tsquery('hire');
```

search apparently non-political ad observer ads (note: inefficient because of the LEFT JOIN / IS NULL)
will find ads whose subs or title contain "Biden" but which don't appear to have a paid for by.

```
    select 
        youtube_videos.id, observed_youtube_ads.title, uploader, youtube_videos.title, alt_title, fulltitle 
    from observed_youtube_ads 
    join youtube_videos on observed_youtube_ads.platformitemid = youtube_videos.id 
    left outer join google_ad_creatives on platformitemid = youtube_ad_id 
    where google_ad_creatives.youtube_ad_id is null 
      and paid_for_by = '' 
      and setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, youtube_videos.title), 'A') || setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, subs), 'B') @@ plainto_tsquery('biden');
```
