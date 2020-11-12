CREATE TABLE public.creative_stats (
    ad_id character varying NOT NULL PRIMARY KEY,
    ad_url character varying NOT NULL,
    ad_type character varying NOT NULL,
    regions character varying NOT NULL,
    advertiser_id character varying NOT NULL,
    advertiser_name character varying NOT NULL,
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
    impressions_max numeric
);

CREATE TABLE public.advertiser_weekly_spend (
    advertiser_id character varying NOT NULL,
    advertiser_name text NOT NULL,
    week_start_date date not null,
    spend_usd integer NOT NULL,
    election_cycle character varying
);
ALTER TABLE ONLY advertiser_weekly_spend ADD CONSTRAINT "ID_PKEY" PRIMARY KEY (advertiser_id,week_start_date);


CREATE TABLE public.google_ad_creatives (
    advertiser_id character varying NOT NULL,
    creative_id character varying NOT NULL,
    ad_type character varying NOT NULL,
    error boolean,
    youtube_ad_id character varying,
    ad_text text,
    image_url text,
    image_urls text[],
    destination text,
)

CREATE TABLE public.advertiser_stats (
    advertiser_id character varying NOT NULL PRIMARY KEY,
    advertiser_name text NOT NULL,
    public_ids_list character varying,
    regions character varying NOT NULL,
    elections character varying NOT NULL,
    total_creatives integer NOT NULL,
    spend_usd integer NOT NULL
);


CREATE TABLE public.youtube_videos (
    id character varying NOT NULL,
    uploader character varying NOT NULL,
    uploader_id character varying NOT NULL,
    uploader_url character varying NOT NULL,
    channel_id character varying NOT NULL,
    channel_url character varying NOT NULL,
    upload_date numeric NOT NULL,
    license boolean,
    creator boolean,
    title character varying NOT NULL,
    alt_title boolean,
    thumbnail character varying NOT NULL,
    description character varying NOT NULL,
    categories boolean,
    tags character varying NOT NULL,
    duration numeric NOT NULL,
    age_limit integer NOT NULL,
    webpage_url character varying NOT NULL,
    view_count numeric NOT NULL,
    like_count boolean,
    dislike_count boolean,
    average_rating numeric,
    is_live boolean,
    display_id character varying NOT NULL,
    format character varying NOT NULL,
    format_id character varying NOT NULL,
    width numeric NOT NULL,
    height numeric NOT NULL,
    resolution boolean,
    fps numeric NOT NULL,
    fulltitle boolean,
    subs character varying,
    subtitle_lang character varying,
    error boolean NOT NULL
);
