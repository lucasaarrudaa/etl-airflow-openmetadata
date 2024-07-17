-- Inserir dados em transformed.transformed_pv
INSERT INTO transformed.transformed_pv (
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
    campaign_id
)
SELECT 
    device_id,
    ip,
    -- Limpar o campaign_link removendo query parameters
    SPLIT_PART(campaign_link, '?', 1) AS cleaned_campaign_link,
    advertising,
    -- Convertendo data_click de string para timestamp
    TO_TIMESTAMP(data_click, 'YYYY-MM-DD HH24:MI:SS'),
    campaign_domain,
    campaign_path,
    advertising_domain,
    advertising_path,
    -- Extrair ad_creative_id se presente
    CASE 
        WHEN POSITION('ad_creative_id=' IN campaign_link) > 0 THEN
            CAST(REGEXP_REPLACE(SUBSTRING(
                campaign_link FROM POSITION('ad_creative_id=' IN campaign_link) + 15 FOR COALESCE(NULLIF(POSITION('&' IN SUBSTRING(campaign_link FROM POSITION('ad_creative_id=' IN campaign_link) + 15)), 0), LENGTH(campaign_link)) - 1), '[^0-9]', '', 'g') AS INTEGER)
        ELSE
            NULL
    END AS ad_creative_id,
    -- Extrair campaign_id se tiver
    CASE 
        WHEN POSITION('campaign_id=' IN campaign_link) > 0 THEN
            CAST(REGEXP_REPLACE(SUBSTRING(
                campaign_link FROM POSITION('campaign_id=' IN campaign_link) + 12 FOR COALESCE(NULLIF(POSITION('&' IN SUBSTRING(campaign_link FROM POSITION('campaign_id=' IN campaign_link) + 12)), 0), LENGTH(campaign_link)) - 1), '[^0-9]', '', 'g') AS INTEGER)
        ELSE
            NULL
    END AS campaign_id
FROM staging.stg_pageview;
