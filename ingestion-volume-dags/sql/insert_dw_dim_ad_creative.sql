INSERT INTO dw.dim_ad_creative (ad_creative_id, ad_creative_name)
SELECT DISTINCT ad_creative_id, ad_creative_name
FROM transformed.aggregated_campaigns
WHERE ad_creative_id IS NOT NULL
AND ad_creative_id NOT IN (SELECT ad_creative_id FROM dw.dim_ad_creative);
