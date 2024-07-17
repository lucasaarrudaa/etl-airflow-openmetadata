CREATE SCHEMA dw;

CREATE TABLE dw.dim_campaign (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_platform VARCHAR(50)
);

CREATE TABLE dw.dim_date (
    date_id SERIAL PRIMARY KEY,
    calendar_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
);

CREATE TABLE dw.dim_ad_creative (
    ad_creative_id INT PRIMARY KEY,
    ad_creative_name VARCHAR(255)
);

CREATE TABLE dw.fact_campaign_performance (
    fact_id SERIAL PRIMARY KEY,
    campaign_id INT,
    date_id INT,
    ad_creative_id INT,
    impressions INT,
    clicks INT,
    cost DECIMAL(10, 2),
    FOREIGN KEY (campaign_id) REFERENCES dw.dim_campaign(campaign_id),
    FOREIGN KEY (date_id) REFERENCES dw.dim_date(date_id),
    FOREIGN KEY (ad_creative_id) REFERENCES dw.dim_ad_creative(ad_creative_id)
);

CREATE TABLE dw.fact_leads (
    lead_id INT PRIMARY KEY,
    campaign_id INT,
    date_id INT,
    ad_creative_id INT,
    FOREIGN KEY (campaign_id) REFERENCES dw.dim_campaign(campaign_id),
    FOREIGN KEY (date_id) REFERENCES dw.dim_date(date_id),
    FOREIGN KEY (ad_creative_id) REFERENCES dw.dim_ad_creative(ad_creative_id)
);
c