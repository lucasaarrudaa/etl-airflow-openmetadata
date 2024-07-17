INSERT INTO dw.dim_date (calendar_date, year, quarter, month, day)
SELECT DISTINCT 
    campaign_date, 
    EXTRACT(YEAR FROM campaign_date), 
    EXTRACT(QUARTER FROM campaign_date), 
    EXTRACT(MONTH FROM campaign_date), 
    EXTRACT(DAY FROM campaign_date)
FROM transformed.aggregated_campaigns;
