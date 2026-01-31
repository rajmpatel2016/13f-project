from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json
import os
import re
import time
from datetime import datetime, date
import requests

# APScheduler for quarterly 13F refresh
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from scrapers.sec_13f_scraper import SUPERINVESTORS, CUSIP_TO_TICKER

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# CUSIP TO TICKER LOOKUP SYSTEM (OpenFIGI API)
# =============================================================================
# OpenFIGI is a free API that maps CUSIPs to tickers
# Rate limit: 25 requests/minute, 100 identifiers per request (no API key)
# With API key: 250 requests/minute
# =============================================================================

CUSIP_CACHE_FILE = "cusip_cache.json"
CUSIP_CACHE = {}  # In-memory cache: cusip -> {"ticker": "AAPL", "name": "Apple Inc"}

def load_cusip_cache():
    global CUSIP_CACHE
    if os.path.exists(CUSIP_CACHE_FILE):
        try:
            with open(CUSIP_CACHE_FILE, 'r') as f:
                CUSIP_CACHE = json.load(f)
            print(f"[CUSIP] Loaded {len(CUSIP_CACHE)} cached mappings")
        except:
            CUSIP_CACHE = {}

def save_cusip_cache():
    try:
        with open(CUSIP_CACHE_FILE, 'w') as f:
            json.dump(CUSIP_CACHE, f)
    except Exception as e:
        print(f"[CUSIP] Failed to save cache: {e}")

def lookup_cusips_openfigi(cusips: list) -> dict:
    """
    Batch lookup CUSIPs via OpenFIGI API.
    Returns dict: cusip -> {"ticker": "AAPL", "name": "Apple Inc"} or None if not found
    """
    if not cusips:
        return {}
    
    # OpenFIGI accepts up to 100 identifiers per request
    results = {}
    
    for i in range(0, len(cusips), 100):
        batch = cusips[i:i+100]
        
        # Build request payload
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]
        
        try:
            r = requests.post(
                "https://api.openfigi.com/v3/mapping",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=10
            )
            
            if r.status_code == 200:
                data = r.json()
                for j, item in enumerate(data):
                    cusip = batch[j]
                    if item and "data" in item and len(item["data"]) > 0:
                        figi_data = item["data"][0]
                        ticker = figi_data.get("ticker", "")
                        name = figi_data.get("name", "")
                        if ticker:
                            results[cusip] = {"ticker": ticker, "name": name}
                            # Also cache by 6-char prefix for flexibility
                            results[cusip[:6]] = {"ticker": ticker, "name": name}
            elif r.status_code == 429:
                print("[CUSIP] Rate limited by OpenFIGI, waiting...")
                time.sleep(60)  # Wait a minute if rate limited
            else:
                print(f"[CUSIP] OpenFIGI error: {r.status_code}")
                
            # Respect rate limits
            time.sleep(0.5)
            
        except Exception as e:
            print(f"[CUSIP] OpenFIGI lookup failed: {e}")
    
    return results

def get_ticker_for_cusip(cusip: str, issuer_name: str = "") -> str:
    """
    Get ticker for a CUSIP. Checks in order:
    1. In-memory cache
    2. Hardcoded mappings
    3. Returns CUSIP prefix if not found (will be resolved in batch later)
    """
    cusip_6 = cusip[:6]
    
    # Check cache first
    if cusip in CUSIP_CACHE:
        return CUSIP_CACHE[cusip].get("ticker", cusip_6)
    if cusip_6 in CUSIP_CACHE:
        return CUSIP_CACHE[cusip_6].get("ticker", cusip_6)
    
    # Check hardcoded mappings
    if cusip_6 in CUSIP_TO_TICKER:
        return CUSIP_TO_TICKER[cusip_6]
    
    # Return prefix - will be resolved later
    return cusip_6

def resolve_unknown_cusips(holdings: list) -> list:
    """
    Take a list of holdings with potentially unknown CUSIPs and resolve them via OpenFIGI.
    Updates the CUSIP_CACHE and returns updated holdings.
    """
    # Find CUSIPs we don't know
    unknown_cusips = []
    for h in holdings:
        ticker = h.get("ticker", "")
        # If ticker looks like a CUSIP (6 chars, has numbers), it's unresolved
        if len(ticker) == 6 and any(c.isdigit() for c in ticker):
            # Get the full CUSIP if available, otherwise use ticker as prefix
            cusip = h.get("cusip", ticker)
            if cusip not in CUSIP_CACHE and cusip[:6] not in CUSIP_CACHE:
                unknown_cusips.append(cusip)
    
    if unknown_cusips:
        print(f"[CUSIP] Looking up {len(unknown_cusips)} unknown CUSIPs via OpenFIGI...")
        new_mappings = lookup_cusips_openfigi(list(set(unknown_cusips)))
        
        if new_mappings:
            CUSIP_CACHE.update(new_mappings)
            save_cusip_cache()
            print(f"[CUSIP] Resolved {len(new_mappings)} new mappings")
            
            # Update holdings with new tickers
            for h in holdings:
                ticker = h.get("ticker", "")
                if len(ticker) == 6 and any(c.isdigit() for c in ticker):
                    cusip = h.get("cusip", ticker)
                    if cusip in CUSIP_CACHE:
                        h["ticker"] = CUSIP_CACHE[cusip]["ticker"]
                    elif cusip[:6] in CUSIP_CACHE:
                        h["ticker"] = CUSIP_CACHE[cusip[:6]]["ticker"]
    
    return holdings

# Load CUSIP cache on startup
load_cusip_cache()

# =============================================================================
# QUARTERLY 13F REFRESH SCHEDULER
# =============================================================================
# Refresh window: 10 days before deadline → 5 days after deadline
# Q4 filing (Feb 14): Feb 4-19
# Q1 filing (May 15): May 5-20
# Q2 filing (Aug 14): Aug 4-19
# Q3 filing (Nov 14): Nov 4-19
# =============================================================================

REFRESH_WINDOWS = [
    (2, 4, 2, 19),   # Q4 filing: Feb 4-19
    (5, 5, 5, 20),   # Q1 filing: May 5-20
    (8, 4, 8, 19),   # Q2 filing: Aug 4-19
    (11, 4, 11, 19), # Q3 filing: Nov 4-19
]

scheduler = BackgroundScheduler()

def is_in_refresh_window() -> bool:
    today = date.today()
    for start_month, start_day, end_month, end_day in REFRESH_WINDOWS:
        if start_month == end_month:
            if today.month == start_month and start_day <= today.day <= end_day:
                return True
    return False

def get_next_refresh_window() -> str:
    today = date.today()
    current_year = today.year
    for start_month, start_day, end_month, end_day in REFRESH_WINDOWS:
        start_date = date(current_year, start_month, start_day)
        end_date = date(current_year, end_month, end_day)
        if end_date < today:
            start_date = date(current_year + 1, start_month, start_day)
            end_date = date(current_year + 1, end_month, end_day)
        if start_date >= today or (start_date <= today <= end_date):
            return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    return "Unknown"

def scheduled_13f_refresh():
    print(f"[Scheduler] Checking refresh window... ({datetime.now()})")
    if is_in_refresh_window():
        print("[Scheduler] In window - starting refresh...")
        do_full_refresh()
        print("[Scheduler] Refresh complete")
    else:
        print(f"[Scheduler] Not in window. Next: {get_next_refresh_window()}")

@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(
        scheduled_13f_refresh,
        CronTrigger(hour=6, minute=0),
        id='quarterly_13f_refresh',
        name='Quarterly 13F Data Refresh',
        replace_existing=True
    )
    scheduler.start()
    print("[Scheduler] Started (daily check at 6:00 AM UTC)")
    print(f"[Scheduler] In refresh window: {is_in_refresh_window()}")
    print(f"[Scheduler] Next window: {get_next_refresh_window()}")

@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown(wait=False)
    print("[Scheduler] Stopped")

CACHE_FILE = "./data/cache.json"
CACHE = {"investors": [], "details": {}, "last_updated": None, "refresh_status": "idle", "refresh_progress": 0, "failed": []}

HEADERS = {
    "User-Agent": "InvestorInsight Research Bot (contact@investorinsight.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, application/xml"
}

def load_cache():
    global CACHE
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            CACHE = json.load(f)

def save_cache():
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(CACHE, f)

load_cache()

@app.get("/")
def root():
    """Serve the frontend"""
    return FileResponse("frontend/index.html")

@app.get("/health")
def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "last_updated": CACHE.get("last_updated"),
        "cached_investors": len(CACHE.get("investors", [])),
        "refresh_status": CACHE.get("refresh_status", "idle"),
        "refresh_progress": CACHE.get("refresh_progress", 0)
    }

@app.get("/api/superinvestors")
def get_superinvestors():
    if CACHE["investors"]:
        return CACHE["investors"]
    return {"error": "No data cached. Call /api/refresh first."}

@app.get("/api/superinvestors/{cik}")
def get_superinvestor(cik: str):
    if cik in CACHE["details"]:
        return CACHE["details"][cik]
    return {"error": "Not found"}

@app.get("/api/debug")
def debug():
    return {
        "superinvestors_count": len(SUPERINVESTORS),
        "first_5": list(SUPERINVESTORS.keys())[:5],
        "last_5": list(SUPERINVESTORS.keys())[-5:]
    }

@app.get("/api/failed")
def get_failed():
    return {
        "failed_count": len(CACHE.get("failed", [])),
        "failed": CACHE.get("failed", [])
    }

@app.get("/api/status")
def get_status():
    return {
        "refresh_status": CACHE.get("refresh_status", "idle"),
        "refresh_progress": CACHE.get("refresh_progress", 0),
        "cached_investors": len(CACHE.get("investors", [])),
        "failed_count": len(CACHE.get("failed", [])),
        "last_updated": CACHE.get("last_updated")
    }

def scrape_one(cik: str, info: dict):
    try:
        cik_padded = cik.zfill(10)
        time.sleep(0.12)
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json", headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return None, "CIK not found"
        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        if not recent:
            return None, "No recent filings"
        
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        
        idx = next((i for i, f in enumerate(forms) if f in ["13F-HR", "13F-HR/A"]), None)
        if idx is None:
            return None, "No 13F-HR filing"
        
        acc = accessions[idx].replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
        
        time.sleep(0.12)
        r = requests.get(index_url, headers=HEADERS, timeout=8)
        matches = re.findall(r'href="([^"]*infotable[^"]*\.xml)"', r.text, re.IGNORECASE)
        if not matches:
            matches = [x for x in re.findall(r'href="([^"]+\.xml)"', r.text, re.IGNORECASE) if 'primary_doc' not in x.lower()]
        if not matches:
            return None, "No XML file found"
        
        xml_url = f"https://www.sec.gov{matches[0]}" if matches[0].startswith('/') else f"{index_url}{matches[0]}"
        
        time.sleep(0.12)
        xml = requests.get(xml_url, headers=HEADERS, timeout=8).text
        
        holdings = []
        # Handle both with and without namespace prefixes (ns1:infoTable or infoTable)
        for table in re.findall(r'<(?:\w+:)?infoTable[^>]*>(.*?)</(?:\w+:)?infoTable>', xml, re.DOTALL | re.IGNORECASE):
            cm = re.search(r'<(?:\w+:)?cusip>([^<]+)</(?:\w+:)?cusip>', table, re.IGNORECASE)
            if not cm:
                continue
            cusip = cm.group(1).strip()
            nm = re.search(r'<(?:\w+:)?nameOfIssuer>([^<]+)</(?:\w+:)?nameOfIssuer>', table, re.IGNORECASE)
            vm = re.search(r'<(?:\w+:)?value>([^<]+)</(?:\w+:)?value>', table, re.IGNORECASE)
            
            # Try multiple patterns for shares - they can be nested in different ways
            sm = None
            # Pattern 1: Direct sshPrnamt
            sm = re.search(r'<(?:\w+:)?sshPrnamt>(\d+)</(?:\w+:)?sshPrnamt>', table, re.IGNORECASE)
            if not sm:
                # Pattern 2: Inside shrsOrPrnAmt wrapper
                shares_block = re.search(r'<(?:\w+:)?shrsOrPrnAmt[^>]*>(.*?)</(?:\w+:)?shrsOrPrnAmt>', table, re.DOTALL | re.IGNORECASE)
                if shares_block:
                    sm = re.search(r'<(?:\w+:)?sshPrnamt>(\d+)</(?:\w+:)?sshPrnamt>', shares_block.group(1), re.IGNORECASE)
            
            pm = re.search(r'<(?:\w+:)?putCall>([^<]+)</(?:\w+:)?putCall>', table, re.IGNORECASE)
            
            name = nm.group(1) if nm else ""
            if pm:
                name = f"{name} ({pm.group(1).upper()})"
            
            # Use new CUSIP lookup system - stores full CUSIP for later resolution
            holdings.append({
                "cusip": cusip,  # Store full CUSIP for OpenFIGI lookup
                "ticker": get_ticker_for_cusip(cusip, name),
                "name": name,
                "value": int(vm.group(1)) if vm else 0,
                "shares": int(sm.group(1)) if sm else 0,
            })
        
        if not holdings:
            return None, "No holdings parsed"
        
        # Resolve any unknown CUSIPs via OpenFIGI API
        holdings = resolve_unknown_cusips(holdings)
        
        total = sum(h["value"] for h in holdings)
        for h in holdings:
            h["pct"] = round((h["value"] / total) * 100, 2) if total > 0 else 0
        holdings.sort(key=lambda x: x["value"], reverse=True)
        
        return {"cik": cik, "name": info["name"], "firm": info["firm"], "value": total, "filing_date": dates[idx], "holdings": holdings}, None
    except Exception as e:
        return None, str(e)

def do_full_refresh():
    global CACHE
    CACHE["refresh_status"] = "running"
    CACHE["refresh_progress"] = 0
    CACHE["failed"] = []
    save_cache()
    
    total = len(SUPERINVESTORS)
    done = 0
    
    for cik, info in SUPERINVESTORS.items():
        result, error = scrape_one(cik, info)
        if result:
            CACHE["details"][cik] = result
        else:
            CACHE["failed"].append({"cik": cik, "name": info["name"], "reason": error})
        done += 1
        CACHE["refresh_progress"] = int((done / total) * 100)
        
        if done % 10 == 0:
            CACHE["investors"] = sorted(
                [{"cik": k, "name": v["name"], "firm": v["firm"], "value": v["value"], "filing_date": v["filing_date"]} 
                 for k, v in CACHE["details"].items()],
                key=lambda x: x["value"], reverse=True
            )
            save_cache()
    
    CACHE["investors"] = sorted(
        [{"cik": k, "name": v["name"], "firm": v["firm"], "value": v["value"], "filing_date": v["filing_date"]} 
         for k, v in CACHE["details"].items()],
        key=lambda x: x["value"], reverse=True
    )
    CACHE["last_updated"] = datetime.now().isoformat()
    CACHE["refresh_status"] = "complete"
    CACHE["refresh_progress"] = 100
    save_cache()

@app.get("/api/refresh")
def refresh_data(background_tasks: BackgroundTasks):
    if CACHE.get("refresh_status") == "running":
        return {
            "status": "already_running",
            "progress": CACHE.get("refresh_progress", 0)
        }
    
    background_tasks.add_task(do_full_refresh)
    
    return {
        "status": "started",
        "message": "Refresh started in background. Check /api/status for progress.",
        "total_investors": len(SUPERINVESTORS)
    }

@app.get("/api/scheduler")
def get_scheduler_status():
    jobs = scheduler.get_jobs()
    job_info = [{"id": j.id, "name": j.name, "next_run": j.next_run_time.isoformat() if j.next_run_time else None} for j in jobs]
    return {
        "scheduler_running": scheduler.running,
        "in_refresh_window": is_in_refresh_window(),
        "next_refresh_window": get_next_refresh_window(),
        "refresh_windows": [
            {"period": "Q4 filings", "window": "Feb 4-19"},
            {"period": "Q1 filings", "window": "May 5-20"},
            {"period": "Q2 filings", "window": "Aug 4-19"},
            {"period": "Q3 filings", "window": "Nov 4-19"},
        ],
        "daily_check_time": "6:00 AM UTC",
        "scheduled_jobs": job_info
    }

@app.get("/api/cusip")
def get_cusip_cache():
    """View CUSIP cache statistics and sample mappings"""
    return {
        "total_cached": len(CUSIP_CACHE),
        "sample_mappings": dict(list(CUSIP_CACHE.items())[:20]),
        "hardcoded_count": len(CUSIP_TO_TICKER)
    }

@app.get("/api/cusip/lookup/{cusip}")
def lookup_single_cusip(cusip: str):
    """Look up a single CUSIP and cache the result"""
    # Check cache first
    if cusip in CUSIP_CACHE:
        return {"source": "cache", "cusip": cusip, "data": CUSIP_CACHE[cusip]}
    if cusip[:6] in CUSIP_CACHE:
        return {"source": "cache", "cusip": cusip, "data": CUSIP_CACHE[cusip[:6]]}
    if cusip[:6] in CUSIP_TO_TICKER:
        return {"source": "hardcoded", "cusip": cusip, "ticker": CUSIP_TO_TICKER[cusip[:6]]}
    
    # Look up via OpenFIGI
    results = lookup_cusips_openfigi([cusip])
    if results:
        return {"source": "openfigi", "cusip": cusip, "data": results.get(cusip) or results.get(cusip[:6])}
    
    return {"source": "not_found", "cusip": cusip, "data": None}

@app.get("/api/debug/cache/{cik}")
def debug_cache(cik: str):
    """See what's actually in the cache for this investor"""
    if cik in CACHE["details"]:
        data = CACHE["details"][cik]
        return {
            "in_cache": True,
            "holdings_count": len(data.get("holdings", [])),
            "data": data
        }
    return {"in_cache": False, "cik": cik}

@app.get("/api/debug/refresh/{cik}")
def debug_refresh_one(cik: str):
    """Force re-scrape a single investor and update cache"""
    if cik not in SUPERINVESTORS:
        return {"error": "CIK not in SUPERINVESTORS list"}
    
    info = SUPERINVESTORS[cik]
    result, error = scrape_one(cik, info)
    
    if result:
        CACHE["details"][cik] = result
        save_cache()
        return {
            "success": True,
            "holdings_count": len(result.get("holdings", [])),
            "data": result
        }
    else:
        return {"success": False, "error": error}

@app.get("/api/debug/scrape/{cik}")
def debug_scrape(cik: str):
    """Debug endpoint to see raw scraping data"""
    if cik not in SUPERINVESTORS:
        return {"error": "CIK not in SUPERINVESTORS list"}
    
    info = SUPERINVESTORS[cik]
    try:
        cik_padded = cik.zfill(10)
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json", headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return {"error": f"CIK lookup failed: {r.status_code}"}
        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        
        idx = next((i for i, f in enumerate(forms) if f in ["13F-HR", "13F-HR/A"]), None)
        if idx is None:
            return {"error": "No 13F-HR filing found"}
        
        acc = accessions[idx].replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
        
        r = requests.get(index_url, headers=HEADERS, timeout=8)
        matches = re.findall(r'href="([^"]*infotable[^"]*\.xml)"', r.text, re.IGNORECASE)
        if not matches:
            matches = [x for x in re.findall(r'href="([^"]+\.xml)"', r.text, re.IGNORECASE) if 'primary_doc' not in x.lower()]
        
        if not matches:
            return {"error": "No XML file found", "index_url": index_url}
        
        xml_url = f"https://www.sec.gov{matches[0]}" if matches[0].startswith('/') else f"{index_url}{matches[0]}"
        xml = requests.get(xml_url, headers=HEADERS, timeout=8).text
        
        # Count raw infoTable entries
        info_tables = re.findall(r'<(?:\w+:)?infoTable[^>]*>(.*?)</(?:\w+:)?infoTable>', xml, re.DOTALL | re.IGNORECASE)
        
        # Parse each one and show what we get
        parsed = []
        for i, table in enumerate(info_tables):
            cm = re.search(r'<(?:\w+:)?cusip>([^<]+)</(?:\w+:)?cusip>', table, re.IGNORECASE)
            nm = re.search(r'<(?:\w+:)?nameOfIssuer>([^<]+)</(?:\w+:)?nameOfIssuer>', table, re.IGNORECASE)
            vm = re.search(r'<(?:\w+:)?value>([^<]+)</(?:\w+:)?value>', table, re.IGNORECASE)
            
            # Try multiple patterns for shares
            sm = re.search(r'<(?:\w+:)?sshPrnamt>(\d+)</(?:\w+:)?sshPrnamt>', table, re.IGNORECASE)
            if not sm:
                shares_block = re.search(r'<(?:\w+:)?shrsOrPrnAmt[^>]*>(.*?)</(?:\w+:)?shrsOrPrnAmt>', table, re.DOTALL | re.IGNORECASE)
                if shares_block:
                    sm = re.search(r'<(?:\w+:)?sshPrnamt>(\d+)</(?:\w+:)?sshPrnamt>', shares_block.group(1), re.IGNORECASE)
            
            parsed.append({
                "index": i,
                "has_cusip": cm is not None,
                "cusip": cm.group(1) if cm else None,
                "name": nm.group(1) if nm else None,
                "value": vm.group(1) if vm else None,
                "shares": sm.group(1) if sm else None,
            })
        
        return {
            "cik": cik,
            "name": info["name"],
            "xml_url": xml_url,
            "info_table_count": len(info_tables),
            "parsed_count": len([p for p in parsed if p["has_cusip"]]),
            "parsed_details": parsed
        }
    except Exception as e:
        return {"error": str(e)}
