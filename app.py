from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from datetime import datetime

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "./data/cache.json"
CACHE = {"investors": [], "details": {}, "last_updated": None}

def load_cache():
    global CACHE
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            CACHE = json.load(f)

def save_cache():
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(CACHE, f)

# Load cache on startup
load_cache()

@app.get("/")
def root():
    return {"status": "healthy", "last_updated": CACHE.get("last_updated")}

@app.get("/api/superinvestors")
def get_superinvestors():
    if CACHE["investors"]:
        return CACHE["investors"]
    return {"error": "No data cached. Call /api/refresh first."}

@app.get("/api/superinvestors/{cik}")
def get_superinvestor(cik: str):
    if cik in CACHE["details"]:
        return CACHE["details"][cik]
    return {"error": "Not found. Call /api/refresh first."}

@app.get("/api/refresh")
def refresh_data():
    """Manually refresh all data from SEC (takes 1-2 minutes)"""
    from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
    
    scraper = SEC13FScraper(data_dir="./data/13f")
    investors = []
    details = {}
    
    for cik, info in SUPERINVESTORS.items():
        try:
            filings = scraper.get_cik_filings(cik, "13F-HR")
            if not filings:
                continue
                
            holdings = scraper.get_13f_holdings(cik, filings[0]["accession_number"])
            if not holdings:
                continue
            
            total_value = sum(h.value for h in holdings)
            for h in holdings:
                if total_value > 0:
                    h.pct_portfolio = round((h.value / total_value) * 100, 2)
            
            holdings.sort(key=lambda x: x.value, reverse=True)
            
            investors.append({
                "cik": cik,
                "name": info["name"],
                "firm": info["firm"],
                "value": total_value,
                "filing_date": filings[0]["filing_date"]
            })
            
            details[cik] = {
                "cik": cik,
                "name": info["name"],
                "firm": info["firm"],
                "value": total_value,
                "filing_date": filings[0]["filing_date"],
                "holdings": [
                    {
                        "ticker": h.ticker or h.cusip[:6],
                        "name": h.issuer_name,
                        "pct": h.pct_portfolio,
                        "shares": h.shares,
                        "value": h.value
                    }
                    for h in holdings
                ]
            }
        except Exception as e:
            print(f"Error scraping {info['name']}: {e}")
            continue
    
    investors.sort(key=lambda x: x["value"], reverse=True)
    
    CACHE["investors"] = investors
    CACHE["details"] = details
    CACHE["last_updated"] = datetime.now().isoformat()
    save_cache()
    
    return {"status": "refreshed", "investors_count": len(investors), "last_updated": CACHE["last_updated"]}
