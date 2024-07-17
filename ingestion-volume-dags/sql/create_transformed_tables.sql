CREATE SCHEMA transformed;

CREATE TABLE transformed.aggregated_campaigns (
    campaign_date DATE,
    campaign_id INT,
    campaign_name VARCHAR(255),
    impressions INT,
    clicks FLOAT,
    cost FLOAT,
    platform VARCHAR(10),
    ad_creative_id INT,
    ad_creative_name VARCHAR(255)
);

CREATE TABLE transformed.leads (
    device_id VARCHAR(255),
    ip VARCHAR(255),
    campaign_link VARCHAR(500),
    advertising VARCHAR(500),
    data_click TIMESTAMP,
    campaign_domain VARCHAR(255),
    campaign_path VARCHAR(255),
    advertising_domain VARCHAR(255),
    advertising_path VARCHAR(255),
    ad_creative_id VARCHAR(255),
    campaign_id INT,
    lead_id BIGINT,
    registered_at TIMESTAMP,
    credit_decision CHAR(1),
    credit_decision_at TIMESTAMP,
    signed_at TIMESTAMP,
    revenue FLOAT,
    unified_campaign_id VARCHAR(255)
);

CREATE TABLE transformed.transformed_pv (
    device_id VARCHAR(255),
    ip VARCHAR(255),
    campaign_link VARCHAR(500),
    advertising VARCHAR(500),
    data_click TIMESTAMP,
    campaign_domain VARCHAR(255),
    campaign_path VARCHAR(255),
    advertising_domain VARCHAR(255),
    advertising_path VARCHAR(255),
    ad_creative_id VARCHAR(255),
    campaign_id INT,
    unified_campaign_id VARCHAR(255)
);
