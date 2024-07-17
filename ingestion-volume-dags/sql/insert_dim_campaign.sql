INSERT INTO dw.dim_campaign (campaign_id, campaign_name, campaign_platform)
SELECT DISTINCT campaign_id, campaign_name, platform
FROM transformed.aggregated_campaigns;
