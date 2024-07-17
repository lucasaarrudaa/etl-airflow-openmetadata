WITH RankedPV AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY device_id
               ORDER BY 
                   CASE WHEN campaign_id IS NOT NULL THEN 0 ELSE 1 END,
                   data_click DESC
           ) AS rn
    FROM transformed.transformed_pv
)
INSERT INTO transformed.leads (
    device_id,
    ip,
    campaign_link,
    advertising,
    data_click,
    campaign_domain,
    campaign_path,
    advertising_domain,
    advertising_path,
    ad_creative_id,
    campaign_id,
    lead_id,
    registered_at,
    credit_decision,
    credit_decision_at,
    signed_at,
    revenue,
    unified_campaign_id
)
SELECT 
    pv.device_id,
    pv.ip,
    pv.campaign_link,
    pv.advertising,
    pv.data_click,
    pv.campaign_domain,
    pv.campaign_path,
    pv.advertising_domain,
    pv.advertising_path,
    pv.ad_creative_id,
    pv.campaign_id,
    l.lead_id,
    l.registered_at,
    l.credit_decision,
    l.credit_decision_at,
    l.signed_at,
    l.revenue,
    pv.unified_campaign_id
FROM RankedPV pv
INNER JOIN staging.stg_leads l ON pv.device_id = l.device_id
WHERE pv.rn = 1;
