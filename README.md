# VC-Sourcing-Tool

Data platform that aggregates venture-capital relevant signals (funding news, YC companies, and more) into an actionable dataset and FastAPI backend.

## Architecture Overview

- `etl/rss_loader.py` – pulls funding-focused RSS feeds, normalises entries, and stores them in SQLite + CSV backup.
- `etl/yc_loader.py` – ingests Y Combinator company data from the YC OSS API.
- `api/main.py` – FastAPI service exposing REST endpoints for news, YC companies, health, and aggregate stats.
- `data/` – SQLite database (`vc_tool.db`) and CSV exports.

## Getting Started

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Ensure the `data/` directory exists (created automatically by loaders).

## ETL Jobs

### RSS Loader

```bash
python etl/rss_loader.py --max-items 50 --since-days 90
```

- Targets multiple startup/news RSS feeds (TechCrunch, EU-Startups, VentureBeat, etc.)
- Funding keyword filtering
- Writes to SQLite (`news` table) and `data/news_clean.csv`

### YC Companies Loader

```bash
python etl/yc_loader.py --max-companies 500
```

- Fetches Y Combinator company metadata via `https://yc-oss.github.io/api/companies/all.json`
- Persists to SQLite (`yc_companies` table)

## API

Run locally:

```bash
uvicorn api.main:app --reload
```

Endpoints:

| Method | Path            | Description                                                         |
| ------ | --------------- | ------------------------------------------------------------------- |
| GET    | `/health`       | Service status + current UTC timestamp                              |
| GET    | `/news`         | Funding news (filters: `source`, `since_days`, `limit`)             |
| GET    | `/yc/companies` | YC companies (filters: `batch`, `industry`, `status`, `limit`)      |
| GET    | `/stats`        | Aggregated counts for news sources and YC batches/industries/status |

Example request:

```bash
curl "http://127.0.0.1:8000/yc/companies?industry=Fintech&status=Active&limit=100"
```

## Data Model Cheatsheet

### `news`

| Column                             | Notes                           |
| ---------------------------------- | ------------------------------- |
| `title`                            | Article headline                |
| `link`                             | Unique URL (UNIQUE)             |
| `source`                           | Feed identifier                 |
| `published_utc`                    | ISO8601 timestamp (nullable)    |
| `company`                          | Heuristic extraction from title |
| `amount_value` / `amount_currency` | Parsed funding amount           |
| `stage`                            | Seed/Series stage if detected   |
| `inserted_at_utc`                  | Ingestion timestamp             |

### `yc_companies`

| Column                                                           | Notes                                         |
| ---------------------------------------------------------------- | --------------------------------------------- |
| `yc_api_id`                                                      | Integer ID from YC OSS API                    |
| `name`, `description`, `batch`, `industry`, `location`, `status` | Core YC metadata                              |
| `industries`                                                     | JSON array (TEXT) of industries               |
| `website_url`, `company_url`                                     | External + YC detail URLs                     |
| `social_links`                                                   | JSON array (TEXT) of detected social/tag URLs |
| `founding_date`                                                  | ISO8601 timestamp derived from `launched_at`  |
| `inserted_at_utc`                                                | Ingestion timestamp                           |

## Development Workflow

1. Update ETL loaders or API endpoints.
2. Run ETL scripts to populate local data.
3. Launch FastAPI via uvicorn for manual testing.
4. Commit and push via git.

```bash
git status
git add <files>
git commit -m "Your message"
git push origin main
```

## Next Steps / Ideas

- ProductHunt and GitHub ingestion pipelines
- AI layer for startup scoring & memo generation
- Streamlit dashboard (`dashboard/`) consuming FastAPI
- Automated scheduling (e.g. with cron or Airflow)

---

Happy sourcing! 🚀
