INSERT INTO dw.fact_leads (lead_id, campaign_id, date_id, ad_creative_id, revenue)
SELECT 
    l.lead_id,
    l.campaign_id,
    d.date_id,
    l.ad_creative_id::INTEGER, 
    l.revenue
FROM 
    transformed.leads l
JOIN 
    dw.dim_date d ON d.calendar_date = 
        CASE 
            WHEN l.data_click::text ~ '^[0-9]{8}$' THEN TO_DATE(l.data_click::text, 'YYYYMMDD')
            ELSE DATE_TRUNC('day', TO_TIMESTAMP(l.data_click::text, 'YYYY-MM-DD HH24:MI:SS'))
        END
WHERE 
    l.data_click IS NOT NULL
    AND (
        (l.data_click::text ~ '^[0-9]{8}$' AND TO_DATE(l.data_click::text, 'YYYYMMDD') BETWEEN '2000-01-01' AND '2099-12-31')
        OR 
        (l.data_click::text ~ '^[0-9]{8}$' = false AND DATE_TRUNC('day', TO_TIMESTAMP(l.data_click::text, 'YYYY-MM-DD HH24:MI:SS')) BETWEEN '2000-01-01' AND '2099-12-31')
    );
