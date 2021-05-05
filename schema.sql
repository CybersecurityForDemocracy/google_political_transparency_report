CREATE TABLE creative_stats (
    ad_id character varying NOT NULL PRIMARY KEY,
    ad_type character varying NOT NULL,
    regions character varying NOT NULL,
    advertiser_id character varying NOT NULL,
    date_range_start date NOT NULL,
    date_range_end date NOT NULL,
    num_of_days numeric NOT NULL,
    spend_usd character varying NOT NULL,
    first_served_timestamp timestamp without time zone,
    last_served_timestamp timestamp without time zone,
    age_targeting character varying NOT NULL,
    gender_targeting character varying NOT NULL,
    geo_targeting_included character varying NOT NULL,
    geo_targeting_excluded character varying NOT NULL,
    spend_range_min_usd numeric NOT NULL,
    spend_range_max_usd numeric,
    impressions_min numeric NOT NULL,
    impressions_max numeric,
    report_date date;
);

CREATE TABLE advertiser_weekly_spend (
    advertiser_id character varying NOT NULL,
    advertiser_name text NOT NULL,
    week_start_date date not null,
    spend_usd integer NOT NULL,
    election_cycle character varying
);
ALTER TABLE ONLY advertiser_weekly_spend ADD CONSTRAINT "ID_PKEY" PRIMARY KEY (advertiser_id,week_start_date);

CREATE TABLE advertiser_regional_spend (
    advertiser_id character varying NOT NULL,
    country text NOT NULL,
    region text NOT NULL,
    spend_usd integer NOT NULL,
    report_date date NOT NULL
);
ALTER TABLE ONLY advertiser_regional_spend ADD CONSTRAINT "ADV_REGIONAL_SPEND_PKEY" PRIMARY KEY (advertiser_id, country, region, report_date);

CREATE TABLE google_ad_creatives (
    advertiser_id character varying NOT NULL,
    ad_id character varying NOT NULL,
    ad_type character varying NOT NULL,
    policy_violation_date date,
    error boolean,
    youtube_ad_id character varying,
    ad_text text,
    image_url text,
    image_urls text[],
    destination text
);
ALTER TABLE ONLY google_ad_creatives ADD CONSTRAINT "CREATIVES_ID_PKEY" PRIMARY KEY (ad_id);
CREATE INDEX idx_creatives_youtube_ad_id ON google_ad_creatives (youtube_ad_id);
CREATE INDEX idx_creatives_youtube_ad_id_null ON google_ad_creatives WHERE youtube_ad_id is null;

CREATE TABLE advertiser_stats (
    advertiser_id character varying NOT NULL PRIMARY KEY,
    advertiser_name text NOT NULL,
    public_ids_list character varying,
    regions character varying NOT NULL,
    elections character varying NOT NULL,
    total_creatives integer NOT NULL,
    spend_usd integer NOT NULL,
    report_date date not null
);


CREATE TABLE youtube_videos (
    id character varying NOT NULL,
    uploader character varying,
    uploader_id character varying,
    uploader_url character varying,
    channel_id character varying,
    channel_url character varying,
    upload_date date,
    license character varying,
    creator character varying,
    title text,
    alt_title character varying,
    thumbnail character varying,
    description character varying,
    categories text[],
    tags character varying,
    duration numeric,
    age_limit integer,
    webpage_url character varying,
    view_count numeric,
    like_count numeric,
    dislike_count numeric,
    average_rating numeric,
    is_live boolean,
    display_id character varying,
    format character varying,
    format_id character varying,
    width numeric,
    height numeric,
    resolution character varying,
    fps numeric,
    fulltitle character varying,
    subs text,
    subtitle_lang character varying,
    error boolean NOT NULL,
    updated_at timestamptz DEFAULT now(),
    video_unavailable boolean NOT NULL,
    video_private boolean
);

CREATE INDEX idx_fts_youtube_videos ON youtube_videos 
USING gin((setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, title), 'A') || 
       setweight(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, subs), 'B')));
alter table youtube_videos add primary key (id);

CREATE TABLE youtube_video_subs (
    id character varying NOT NULL,
    subs text,
    subtitle_lang character varying,
    asr boolean
);
alter table youtube_video_subs add primary key (id);
CREATE INDEX idx_fts_youtube_videos ON youtube_video_subs 
USING gin(to_tsvector(CASE subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, subs));
-- run just once.
-- INSERT INTO youtube_video_subs SELECT id, subs, subtitle_lang, true FROM youtube_videos;


CREATE SERVER observations 
 FOREIGN DATA WRAPPER postgres_fdw
 OPTIONS (dbname 'observations');


-- CREATE USER MAPPING for CENSORED
-- SERVER observations
-- OPTIONS (user 'CENSORED', password 'CENSORED');


CREATE FOREIGN TABLE observed_youtube_ads (
        id varchar(16),
        video boolean,
        time_of_day boolean,
        general_location  boolean,
        activity boolean,
        similarity boolean,
        age boolean,
        interests_estimation boolean,
        general_location_estimation boolean,
        gender boolean,
        income_estimation boolean,
        parental_status_estimation boolean,
        websites_youve_visited boolean,
        approximate_location boolean,
        activity_eg_searches boolean,
        website_topics boolean,
        age_estimation boolean,
        gender_estimation boolean,


        title text,
        paid_for_by text,
        targeting_on boolean,
        advertiser text,
        itemType text,
        itemId text,
        platformItemId text,
        observedAt timestamp,
        hostVideoId text,
        hostVideoUrl text,
        hostVideoChannelId text,
        hostVideoAuthor text,
        hostVideoTitle text,
        creative text,
        reasons text,
        lang text,
        homeownership_status_estimation boolean,
        company_size_estimation boolean,
        job_industry_estimation boolean,
        marital_status_estimation boolean,
        education_status_estimation boolean,
        visit_to_advertisers_website_or_app boolean,
        search_terms boolean
  )
SERVER observations
OPTIONS (schema_name 'observations', table_name 'youtube_ads')


create table models (model_id serial PRIMARY KEY, created_at timestamptz default now(), location text, model_name text, vocab_path text, encoder_path text);

create table inference_values (youtube_ad_id varchar REFERENCES youtube_videos (id), model_id bigint REFERENCES models (model_id), value real, PRIMARY KEY (model_id, youtube_ad_id));

create table region_populations (region text, region_abbr varchar(2), population int);
-- ccs1 $ \copy region_populations from '/home/jmerrill/US populations, 2019 - populations by state.csv' with csv header;
-- that's from: https://docs.google.com/spreadsheets/d/1PEAcSwPTTBV12I5OQwh_qtuUsqeUJmUm8JNFQ3iHVpc/edit#gid=1891095703