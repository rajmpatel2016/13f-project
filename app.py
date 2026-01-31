from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import re
import time
from datetime import datetime
import requests

from scrapers.sec_13f_scraper import SUPERINVESTORS, CUSIP_TO_TICKER

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "./data/cache.json"
CACHE = {"investors": [], "details": {}, "last_updated": None, "refresh_status": "idle", "refresh_progress": 0}

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

@app.get("/api/status")
def get_status():
    return {
        "refresh_status": CACHE.get("refresh_status", "idle"),
        "refresh_progress": CACHE.get("refresh_progress", 0),
        "cached_investors": len(CACHE.get("investors", [])),
        "last_updated": CACHE.get("last_updated")
    }

def scrape_one(cik: str, info: dict):
    try:
        cik_padded = cik.zfill(10)
        time.sleep(0.12)
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json", headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        if not recent:
            return None
        
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        
        idx = next((i for i, f in enumerate(forms) if f in ["13F-HR", "13F-HR/A"]), None)
        if idx is None:
            return None
        
        acc = accessions[idx].replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
        
        time.sleep(0.12)
        r = requests.get(index_url, headers=HEADERS, timeout=8)
        matches = re.findall(r'href="([^"]*infotable[^"]*\.xml)"', r.text, re.IGNORECASE)
        if not matches:
            matches = [x for x in re.findall(r'href="([^"]+\.xml)"', r.text, re.IGNORECASE) if 'primary_doc' not in x.lower()]
        if not matches:
            return None
        
        xml_url = f"https://www.sec.gov{matches[0]}" if matches[0].startswith('/') else f"{index_url}{matches[0]}"
        
        time.sleep(0.12)
        xml = requests.get(xml_url, headers=HEADERS, timeout=8).text
        
        holdings = []
        for table in re.findall(r'<infoTable>(.*?)</infoTable>', xml, re.DOTALL):
            cm = re.search(r'<cusip>([^<]+)</cusip>', table)
            if not cm:
                continue
            cusip = cm.group(1)
            nm = re.search(r'<nameOfIssuer>([^<]+)</nameOfIssuer>', table)
            vm = re.search(r'<value>([^<]+)</value>', table)
            sm = re.search(r'<sshPrnamt>([^<]+)</sshPrnamt>', table)
            pm = re.search(r'<putCall>([^<]+)</putCall>', table)
            
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
            return None
        
        total = sum(h["value"] for h in holdings)
        for h in holdings:
            h["pct"] = round((h["value"] / total) * 100, 2) if total > 0 else 0
        holdings.sort(key=lambda x: x["value"], reverse=True)
        
        return {"cik": cik, "name": info["name"], "firm": info["firm"], "value": total, "filing_date": dates[idx], "holdings": holdings}
    except:
        return None

def do_full_refresh():
    global CACHE
    CACHE["refresh_status"] = "running"
    CACHE["refresh_progress"] = 0
    save_cache()
    
    total = len(SUPERINVESTORS)
    done = 0
    
    for cik, info in SUPERINVESTORS.items():
        result = scrape_one(cik, info)
        if result:
            CACHE["details"][cik] = result
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
