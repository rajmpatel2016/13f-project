from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import re
import time
from datetime import datetime
import requests

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "./data/cache.json"
CACHE = {"investors": [], "details": {}, "last_updated": None}

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
    from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
    
    scraper = SEC13FScraper(data_dir="./data/13f")
    investors = []
    details = {}
    
    for cik, info in SUPERINVESTORS.items():
        try:
            filings = scraper.get_cik_filings(cik, "13F-HR")
            if not filings:
                continue
            
            accession = filings[0]["accession_number"].replace("-", "")
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
            
            time.sleep(0.15)
            response = requests.get(index_url, headers=HEADERS, timeout=15)
            xml_pattern = r'href="([^"]*infotable[^"]*\.xml)"'
            matches = re.findall(xml_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                xml_pattern = r'href="([^"]+\.xml)"'
                all_xml = re.findall(xml_pattern, response.text, re.IGNORECASE)
                matches = [x for x in all_xml if 'primary_doc' not in x.lower()]
            
            if not matches:
                continue
            
            xml_file = matches[0]
            if xml_file.startswith('/'):
                xml_url = f"https://www.sec.gov{xml_file}"
            else:
                xml_url = f"{index_url}{xml_file}"
            
            time.sleep(0.15)
            xml_response = requests.get(xml_url, headers=HEADERS, timeout=15)
            xml_content = xml_response.text
            
            holdings = []
            info_tables = re.findall(r'<infoTable>(.*?)</infoTable>', xml_content, re.DOTALL)
            
            for table in info_tables:
                cusip_match = re.search(r'<cusip>([^<]+)</cusip>', table)
                if not cusip_match:
                    continue
                
                cusip = cusip_match.group(1)
                name_match = re.search(r'<nameOfIssuer>([^<]+)</nameOfIssuer>', table)
                value_match = re.search(r'<value>([^<]+)</value>', table)
                shares_match = re.search(r'<sshPrnamt>([^<]+)</sshPrnamt>', table)
                putcall_match = re.search(r'<putCall>([^<]+)</putCall>', table)
                
                name = name_match.group(1) if name_match else ""
                putcall = putcall_match.group(1).upper() if putcall_match else ""
                if putcall:
                    name = f"{name} ({putcall})"
                
                ticker = scraper.cusip_to_ticker.get(cusip[:6]) or cusip[:6]
                
                holdings.append({
                    "ticker": ticker,
                    "name": name,
                    "value": int(value_match.group(1)) if value_match else 0,
                    "shares": int(shares_match.group(1)) if shares_match else 0,
                })
            
            if not holdings:
                continue
            
            total_value = sum(h["value"] for h in holdings)
            for h in holdings:
                h["pct"] = round((h["value"] / total_value) * 100, 2) if total_value > 0 else 0
            
            holdings.sort(key=lambda x: x["value"], reverse=True)
            
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
                "holdings": holdings
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
