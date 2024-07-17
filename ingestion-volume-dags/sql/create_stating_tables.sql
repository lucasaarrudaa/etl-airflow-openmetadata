CREATE TABLE staging.stg_facebook(
    campaign_date timestamp without time zone,
    facebook_campaign_id character varying(255) COLLATE pg_catalog."default",
    campaign_name character varying(255) COLLATE pg_catalog."default",
    impressions integer,
    clicks numeric(10,2),
    cost numeric(10,2)
);

CREATE TABLE staging.stg_google(
    campaign_date timestamp without time zone,
    google_campaign_id character varying(255) COLLATE pg_catalog."default",
    campaign_name character varying(255) COLLATE pg_catalog."default",
    ad_creative_id integer,
    ad_creative_name character varying(255) COLLATE pg_catalog."default",
    impressions integer,
    clicks numeric(10,2),
    cost numeric(10,2)
);

CREATE TABLE staging.stg_leads(
    device_id character varying(255) COLLATE pg_catalog."default",
    lead_id integer,
    registered_at timestamp without time zone,
    credit_decision character varying(255) COLLATE pg_catalog."default",
    credit_decision_at timestamp without time zone,
    signed_at timestamp without time zone,
    revenue numeric(10,2)
)
;

CREATE TABLE staging.stg_pageview(
    device_id character varying(255) COLLATE pg_catalog."default",
    ip character varying(255) COLLATE pg_catalog."default",
    campaign_link character varying(255) COLLATE pg_catalog."default",
    advertising character varying(255) COLLATE pg_catalog."default",
    data_click character varying(255) COLLATE pg_catalog."default",
    campaign_domain character varying(255) COLLATE pg_catalog."default",
    campaign_path character varying(255) COLLATE pg_catalog."default",
    advertising_domain character varying(255) COLLATE pg_catalog."default",
    advertising_path character varying(255) COLLATE pg_catalog."default"
);