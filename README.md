# InvestorInsight

Track the stock trades of superinvestors (via SEC 13F filings) and Congress members (via STOCK Act disclosures).

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize and seed database
python seed_database.py

# 3. Start API server
uvicorn api.main_db:app --reload --port 8000

# 4. Open frontend/index.html in browser
```

## Scheduled Scraping (Production)

```bash
# Start Redis
redis-server

# Start Celery worker
celery -A scheduler.celery_config worker --loglevel=info

# Start Celery beat scheduler
celery -A scheduler.celery_config beat --loglevel=info
```

### Schedule

| Task | Schedule |
|------|----------|
| SEC 13F | Daily in **Feb, May, Aug, Nov** (filing months) |
| Congress trades | Daily 7 PM ET |
| Net worth | Monthly (1st) |

## CLI Commands

```bash
python cli.py init-db              # Initialize database
python cli.py scrape 13f           # SEC 13F filings
python cli.py scrape congress      # Congress trades  
python cli.py scrape networth      # Net worth reports
python cli.py scrape all           # Run all scrapers
python cli.py add-investor <cik>   # Add investor to track
python cli.py status               # View scraper job status
python cli.py stats                # Database statistics
```

## API Endpoints

### Superinvestors
- `GET /api/superinvestors` - List all (sorted by AUM)
- `GET /api/superinvestors/{cik}` - Investor detail + holdings
- `GET /api/superinvestors/{cik}/history` - Historical filings

### Congress
- `GET /api/congress/members` - List all members
- `GET /api/congress/members/{id}` - Member detail
- `GET /api/congress/members/{id}/trades` - Member's trades
- `GET /api/congress/members/{id}/networth` - Net worth
- `GET /api/congress/trades` - Recent trades (filterable)

### Insights
- `GET /api/insights/aggregated` - Top buys/sells/holdings
- `GET /api/insights/stock/{ticker}` - Who owns this stock?

## Database

SQLite (no additional services required):
```
data/investorinsight.db
```

Tables:
- `superinvestors` → `filings_13f` → `holdings`
- `congress_members` → `congress_trades` + `net_worth_reports`
- `scraper_jobs` (tracking)

## Docker (Optional)

```bash
docker-compose up -d
# API: http://localhost:8000
# Flower: http://localhost:5555
```
