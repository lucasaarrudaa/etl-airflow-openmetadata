INSERT INTO transformed.aggregated_campaigns (
    campaign_date,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    platform,
    ad_creative_id,
    ad_creative_name
)
SELECT 
    campaign_date,
    CAST(facebook_campaign_id AS INT) AS campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    'fb' AS platform,
    NULL AS ad_creative_id,
    NULL AS ad_creative_name
FROM 
    staging.stg_facebook

UNION ALL

SELECT 
    campaign_date,
    CAST(google_campaign_id AS INT) AS campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    'ggl' AS platform,
    ad_creative_id,
    ad_creative_name
FROM 
    staging.stg_google;
