-- ============================================================
-- PARFUMELITE ADS ANALYTICS - D1 DATABASE SCHEMA
-- Star Schema Design for Facebook/IG Ads Performance
-- ============================================================

-- Enable Foreign Keys
PRAGMA foreign_keys = ON;

-- ============================================================
-- METADATA TABLES (Required for D1 documentation)
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata_tables (
    table_name TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS metadata_columns (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    description TEXT NOT NULL,
    data_type TEXT,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (table_name) REFERENCES metadata_tables(table_name) ON DELETE CASCADE
);

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- dim_date: Pre-populated date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,          -- Format: YYYYMMDD
    full_date TEXT NOT NULL UNIQUE,        -- ISO format: YYYY-MM-DD
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,          -- 1=Monday, 7=Sunday
    day_name TEXT NOT NULL,
    is_weekend INTEGER DEFAULT 0
);

-- dim_campaign: Campaign hierarchy
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_name TEXT NOT NULL UNIQUE,
    platform TEXT DEFAULT 'Facebook',       -- Facebook, Instagram
    objective TEXT,                          -- Impression, Visit, Mess
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_campaign_name ON dim_campaign(campaign_name);

-- dim_age_group: Age demographics
CREATE TABLE IF NOT EXISTS dim_age_group (
    age_id INTEGER PRIMARY KEY AUTOINCREMENT,
    age_range TEXT NOT NULL UNIQUE          -- 18-24, 25-34, 35-44, etc.
);

-- dim_gender: Gender demographics
CREATE TABLE IF NOT EXISTS dim_gender (
    gender_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gender TEXT NOT NULL UNIQUE             -- female, male, unknown
);

-- dim_region: Geographic regions
CREATE TABLE IF NOT EXISTS dim_region (
    region_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name TEXT NOT NULL UNIQUE,
    region_type TEXT DEFAULT 'Province'     -- Province, City
);

CREATE INDEX IF NOT EXISTS idx_dim_region_name ON dim_region(region_name);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- fact_ads_performance: Main ads metrics (from Auto sheet)
-- Granularity: 1 row = 1 ad × 1 day × 1 action_type
CREATE TABLE IF NOT EXISTS fact_ads_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    adset_name TEXT,
    ad_name TEXT,
    indicator TEXT,                         -- reach, profile_visit_view, etc.
    action_key TEXT,                        -- Detailed action type
    amount_spent REAL DEFAULT 0,
    results INTEGER DEFAULT 0,
    cost_per_result REAL,
    impressions INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_ads_date ON fact_ads_performance(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_ads_campaign ON fact_ads_performance(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fact_ads_date_campaign ON fact_ads_performance(date_key, campaign_id);

-- fact_ads_demographics: Age/Gender breakdown
-- Granularity: 1 row = 1 campaign × 1 day × 1 age × 1 gender
CREATE TABLE IF NOT EXISTS fact_ads_demographics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    action_key TEXT,
    age_id INTEGER NOT NULL,
    gender_id INTEGER NOT NULL,
    spend REAL DEFAULT 0,
    impressions INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),
    FOREIGN KEY (age_id) REFERENCES dim_age_group(age_id),
    FOREIGN KEY (gender_id) REFERENCES dim_gender(gender_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_demo_date ON fact_ads_demographics(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_demo_campaign ON fact_ads_demographics(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fact_demo_age_gender ON fact_ads_demographics(age_id, gender_id);

-- fact_ads_regions: Geographic breakdown
-- Granularity: 1 row = 1 campaign × 1 day × 1 region
CREATE TABLE IF NOT EXISTS fact_ads_regions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    spend REAL DEFAULT 0,
    impressions INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_region_date ON fact_ads_regions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_region_campaign ON fact_ads_regions(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fact_region_region ON fact_ads_regions(region_id);

-- ============================================================
-- SUMMARY TABLES (Pre-aggregated for Dashboard)
-- ============================================================

-- summary_daily: Daily aggregated metrics
CREATE TABLE IF NOT EXISTS summary_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL,
    total_spend REAL DEFAULT 0,
    total_impressions INTEGER DEFAULT 0,
    total_results INTEGER DEFAULT 0,
    campaign_count INTEGER DEFAULT 0,
    avg_cpr REAL,                           -- Cost per result
    avg_cpm REAL,                           -- Cost per 1000 impressions
    updated_at TEXT DEFAULT (datetime('now')),
    
    UNIQUE(date_key)
);

CREATE INDEX IF NOT EXISTS idx_summary_daily_date ON summary_daily(date_key);

-- summary_campaign: Campaign-level aggregates
CREATE TABLE IF NOT EXISTS summary_campaign (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    total_spend REAL DEFAULT 0,
    total_impressions INTEGER DEFAULT 0,
    total_results INTEGER DEFAULT 0,
    avg_cpr REAL,
    updated_at TEXT DEFAULT (datetime('now')),
    
    UNIQUE(campaign_id, date_key),
    FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_summary_campaign ON summary_campaign(campaign_id, date_key);

-- ============================================================
-- ETL TRACKING
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,                   -- 'n8n', 'manual', 'api'
    table_name TEXT NOT NULL,
    rows_processed INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',          -- pending, success, error
    error_message TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT
);

-- ============================================================
-- METADATA INSERTS
-- ============================================================

INSERT OR IGNORE INTO metadata_tables VALUES 
    ('dim_date', 'Date dimension for time-based analysis', datetime('now')),
    ('dim_campaign', 'Campaign hierarchy dimension', datetime('now')),
    ('dim_age_group', 'Age group demographics dimension', datetime('now')),
    ('dim_gender', 'Gender demographics dimension', datetime('now')),
    ('dim_region', 'Geographic region dimension', datetime('now')),
    ('fact_ads_performance', 'Main ads performance fact table (ad-level daily)', datetime('now')),
    ('fact_ads_demographics', 'Demographics breakdown fact table', datetime('now')),
    ('fact_ads_regions', 'Geographic breakdown fact table', datetime('now')),
    ('summary_daily', 'Pre-aggregated daily summary', datetime('now')),
    ('summary_campaign', 'Pre-aggregated campaign summary', datetime('now')),
    ('etl_logs', 'ETL job tracking and logging', datetime('now'));

-- Pre-populate dim_age_group
INSERT OR IGNORE INTO dim_age_group (age_range) VALUES 
    ('13-17'), ('18-24'), ('25-34'), ('35-44'), ('45-54'), ('55-64'), ('65+');

-- Pre-populate dim_gender
INSERT OR IGNORE INTO dim_gender (gender) VALUES 
    ('female'), ('male'), ('unknown');

-- ============================================================
-- VIEWS FOR DASHBOARD QUERIES
-- ============================================================

-- View: Daily overview
CREATE VIEW IF NOT EXISTS v_daily_overview AS
SELECT 
    d.full_date,
    d.day_name,
    COALESCE(s.total_spend, 0) as spend,
    COALESCE(s.total_impressions, 0) as impressions,
    COALESCE(s.total_results, 0) as results,
    COALESCE(s.avg_cpm, 0) as cpm,
    COALESCE(s.avg_cpr, 0) as cpr
FROM dim_date d
LEFT JOIN summary_daily s ON d.date_key = s.date_key
ORDER BY d.full_date DESC;

-- View: Campaign performance
CREATE VIEW IF NOT EXISTS v_campaign_performance AS
SELECT 
    c.campaign_name,
    c.objective,
    SUM(f.amount_spent) as total_spend,
    SUM(f.impressions) as total_impressions,
    SUM(f.results) as total_results,
    CASE WHEN SUM(f.results) > 0 
        THEN ROUND(SUM(f.amount_spent) / SUM(f.results), 2) 
        ELSE 0 END as avg_cpr,
    CASE WHEN SUM(f.impressions) > 0 
        THEN ROUND(SUM(f.amount_spent) / SUM(f.impressions) * 1000, 2) 
        ELSE 0 END as cpm
FROM dim_campaign c
LEFT JOIN fact_ads_performance f ON c.campaign_id = f.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.objective;

-- View: Demographics summary
CREATE VIEW IF NOT EXISTS v_demographics_summary AS
SELECT 
    a.age_range,
    g.gender,
    SUM(f.spend) as total_spend,
    SUM(f.impressions) as total_impressions,
    ROUND(SUM(f.spend) * 100.0 / (SELECT SUM(spend) FROM fact_ads_demographics), 2) as spend_pct
FROM fact_ads_demographics f
JOIN dim_age_group a ON f.age_id = a.age_id
JOIN dim_gender g ON f.gender_id = g.gender_id
GROUP BY a.age_range, g.gender
ORDER BY a.age_range, g.gender;

-- View: Region summary
CREATE VIEW IF NOT EXISTS v_region_summary AS
SELECT 
    r.region_name,
    SUM(f.spend) as total_spend,
    SUM(f.impressions) as total_impressions,
    ROUND(SUM(f.spend) * 100.0 / (SELECT SUM(spend) FROM fact_ads_regions), 2) as spend_pct
FROM fact_ads_regions f
JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region_id, r.region_name
ORDER BY total_spend DESC;
