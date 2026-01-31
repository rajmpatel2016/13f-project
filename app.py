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
            cusip = cm.group(1)
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
            
            holdings.append({
                "ticker": CUSIP_TO_TICKER.get(cusip[:6]) or cusip[:6],
                "name": name,
                "value": int(vm.group(1)) if vm else 0,
                "shares": int(sm.group(1)) if sm else 0,
            })
        
        if not holdings:
            return None, "No holdings parsed"
        
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
