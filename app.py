from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPERINVESTORS = [
    {"cik": "1067983", "name": "Warren Buffett", "firm": "Berkshire Hathaway", "value": 267000000000},
    {"cik": "1336528", "name": "Bill Ackman", "firm": "Pershing Square", "value": 14600000000},
    {"cik": "1649339", "name": "Michael Burry", "firm": "Scion Asset Management", "value": 55000000},
]

CONGRESS = [
    {"bioguide_id": "P000197", "name": "Nancy Pelosi", "party": "D", "chamber": "House", "state": "CA", "trades": 42},
    {"bioguide_id": "T000278", "name": "Tommy Tuberville", "party": "R", "chamber": "Senate", "state": "AL", "trades": 89},
]

@app.get("/")
def root():
    return {"name": "InvestorInsight API", "status": "online", "version": "NEW123"}

@app.get("/api/superinvestors")
def get_superinvestors():
    return SUPERINVESTORS

@app.get("/api/congress/members")
def get_congress():
    return CONGRESS

@app.get("/api/test-sec")
def test_sec():
    try:
        url = "https://data.sec.gov/submissions/CIK0001067983.json"
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        return {"status": "ok", "name": data.get("name"), "cik": data.get("cik")}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/debug-filing")
def debug_filing():
    """See what files are in Buffett's latest 13F filing"""
    import re
    try:
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        
        # Get latest filing info
        url = "https://data.sec.gov/submissions/CIK0001067983.json"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        
        # Find latest 13F-HR
        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        accessions = filings.get("accessionNumber", [])
        
        latest_13f = None
        for i, form in enumerate(forms):
            if "13F-HR" in form:
                latest_13f = accessions[i]
                break
        
        if not latest_13f:
            return {"status": "error", "message": "No 13F found"}
        
        # Get filing index page
        acc_formatted = latest_13f.replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/1067983/{acc_formatted}/"
        
        r2 = requests.get(index_url, headers=headers, timeout=10)
        
        # Find all XML files
        xml_files = re.findall(r'href="([^"]+\.xml)"', r2.text, re.IGNORECASE)
        all_files = re.findall(r'href="([^"]+)"', r2.text)
        
        return {
            "status": "ok",
            "accession": latest_13f,
            "index_url": index_url,
            "xml_files": xml_files,
            "all_files": [f for f in all_files if not f.startswith("?") and not f.startswith("/")]
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}


@app.get("/api/test-scraper")
def test_scraper(cik: str = "1067983"):
    try:
        from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
        
        if cik not in SUPERINVESTORS:
            return {
                "status": "error",
                "message": f"CIK {cik} not tracked",
                "available": list(SUPERINVESTORS.keys())[:5]
            }
        
        scraper = SEC13FScraper(data_dir="./data/13f")
        filings = scraper.get_cik_filings(cik, "13F-HR")
        
        if not filings:
            return {
                "status": "error", 
                "message": "No 13F filings found",
                "cik": cik,
                "debug": "get_cik_filings returned empty list"
            }
        
        latest = filings[0]
        holdings = scraper.get_13f_holdings(cik, latest["accession_number"])
        
        if not holdings:
            return {
                "status": "partial",
                "message": "Found filing but couldn't parse holdings",
                "filing": latest,
                "debug": "get_13f_holdings returned empty"
            }
        
        total_value = sum(h.value for h in holdings)
        
        return {
            "status": "success",
            "investor": SUPERINVESTORS[cik]["name"],
            "firm": SUPERINVESTORS[cik]["firm"],
            "filing_date": latest["filing_date"],
            "total_value_thousands": total_value,
            "positions": len(holdings),
            "top_5": [
                {"ticker": h.ticker or h.cusip[:6], "value": h.value, "pct": h.pct_portfolio}
                for h in sorted(holdings, key=lambda x: x.value, reverse=True)[:5]
            ]
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}
