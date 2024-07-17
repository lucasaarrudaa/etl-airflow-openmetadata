-- ¿Cuál fue la campaña más cara?
SELECT 
    c.campaign_name, 
    SUM(f.cost) AS total_cost
FROM 
    dw.fact_campaign_performance f
JOIN 
    dw.dim_campaign c ON f.campaign_id = c.campaign_id
GROUP BY 
    c.campaign_name
ORDER BY 
    total_cost DESC
LIMIT 1;

-- ¿Cuál fue la campaña más lucrativa?
SELECT 
    c.campaign_name, 
    SUM(l.revenue) - SUM(f.cost) AS profit
FROM 
    dw.fact_leads l
JOIN 
    dw.dim_campaign c ON l.campaign_id = c.campaign_id
JOIN 
    dw.fact_campaign_performance f ON l.campaign_id = f.campaign_id
GROUP BY 
    c.campaign_name
ORDER BY 
    profit DESC
LIMIT 1;

-- ¿Cuál anuncio es el más eficaz en términos de clics?
SELECT 
    a.ad_creative_name, 
    SUM(f.clicks) AS total_clicks
FROM 
    dw.fact_campaign_performance f
JOIN 
    dw.dim_ad_creative a ON f.ad_creative_id = a.ad_creative_id
GROUP BY 
    a.ad_creative_name
ORDER BY 
    total_clicks DESC
LIMIT 1;

-- ¿Cuál anuncio es el más eficaz en términos de generación de leads?
SELECT 
    a.ad_creative_name, 
    COUNT(l.lead_id) AS total_leads
FROM 
    dw.fact_leads l
JOIN 
    dw.dim_ad_creative a ON l.ad_creative_id = a.ad_creative_id
GROUP BY 
    a.ad_creative_name
ORDER BY 
    total_leads DESC
LIMIT 1;
