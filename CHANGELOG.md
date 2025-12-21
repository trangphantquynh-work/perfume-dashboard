# Changelog

## [2025-12-22] - n8n to Cloudflare D1 Integration

### Added

#### n8n Workflows for Cloudflare D1
Thêm 3 workflow n8n mới để tự động đẩy data Facebook Ads vào Cloudflare D1 database thay vì Google Sheets:

| Workflow | File | API Endpoint | Schedule |
|----------|------|--------------|----------|
| Facebook Ads Daily | `n8n/Parfumelite_Facebook_Ads_Daily_Cloudflare.json` | `/api/ingest/performance` | 6:01 AM |
| Age & Gender | `n8n/Parfumelite_Age_Gender_Cloudflare.json` | `/api/ingest/demographics` | 6:30 AM |
| Region | `n8n/Parfumelite_Region_Cloudflare.json` | `/api/ingest/regions` | 6:30 AM |

#### Data Flow
```
Facebook Ads API
    ↓
n8n Workflow (transform data)
    ↓
Cloudflare Worker API (POST)
    ↓
Cloudflare D1 Database
    ↓
Dashboard (realtime)
```

### Fixed

#### Worker API Responses
- Fixed `healthCheck()` function to return proper `Response` object
- Fixed `ingestPerformance()`, `ingestDemographics()`, `ingestRegions()` to return `jsonResponse()` instead of plain objects
- Fixed validation error responses to use `jsonResponse()` with status 400

### Technical Details

#### Worker URL
```
https://parfumelite-dashboard.duongthien408.workers.dev
```

#### API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/overview` | KPI overview |
| GET | `/api/daily` | Daily trend |
| GET | `/api/campaigns` | Top campaigns |
| GET | `/api/demographics` | Age & Gender breakdown |
| GET | `/api/regions` | Region breakdown |
| GET | `/api/breakdown` | Channel & Objective breakdown |
| POST | `/api/ingest/performance` | Ingest ads performance data |
| POST | `/api/ingest/demographics` | Ingest demographics data |
| POST | `/api/ingest/regions` | Ingest regions data |

#### Data Schema (D1)
- `fact_ads_performance` - Daily ads metrics
- `fact_ads_demographics` - Age/Gender breakdown
- `fact_ads_regions` - Geographic breakdown
- `dim_campaign`, `dim_date`, `dim_age_group`, `dim_gender`, `dim_region` - Dimension tables

### How to Use

1. Import workflow JSON files vào n8n
2. Verify Facebook credentials (`FB Ánh Vy`) đã được link
3. Activate workflows
4. Data sẽ tự động sync mỗi ngày lúc 6:01 AM và 6:30 AM (GMT+7)

### Data Coverage
- Current data range: **14/10/2025 - 21/12/2025**
- Auto-updated daily via n8n scheduled triggers
