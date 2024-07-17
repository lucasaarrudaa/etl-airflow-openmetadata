INSERT INTO dw.fact_campaign_performance (campaign_id, date_id, ad_creative_id, impressions, clicks, cost)
SELECT 
    a.campaign_id,
    d.date_id,
    a.ad_creative_id,
    a.impressions,
    a.clicks,
    a.cost
FROM 
    transformed.aggregated_campaigns a
JOIN 
    dw.dim_date d ON a.campaign_date = d.calendar_date;