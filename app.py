from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import re

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"name": "InvestorInsight API", "status": "online", "version": "NEW123"}

@app.get("/api/debug-xml")
def debug_xml():
    """See what XML files exist and what's in them"""
    try:
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        
        # Burry's filing index
        index_url = "https://www.sec.gov/Archives/edgar/data/1649339/000164933925000007/"
        r = requests.get(index_url, headers=headers, timeout=15)
        
        # Find all XML files
        xml_files = re.findall(r'href="([^"]+\.xml)"', r.text, re.IGNORECASE)
        
        # Try to fetch the first non-primary XML
        xml_content = None
        xml_url = None
        for f in xml_files:
            if 'primary_doc' not in f.lower():
                if f.startswith('/'):
                    xml_url = f"https://www.sec.gov{f}"
                else:
                    xml_url = f"{index_url}{f}"
                r2 = requests.get(xml_url, headers=headers, timeout=15)
                xml_content = r2.text[:1000]
                break
        
        return {
            "status": "ok",
            "index_url": index_url,
            "xml_files_found": xml_files,
            "fetched_url": xml_url,
            "xml_preview": xml_content
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}

@app.get("/api/test-scraper")
def test_scraper(cik: str = "1649339"):
    try:
        from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
        
        if cik not in SUPERINVESTORS:
            return {"status": "error", "message": f"CIK {cik} not tracked"}
        
        scraper = SEC13FScraper(data_dir="./data/13f")
        filings = scraper.get_cik_filings(cik, "13F-HR")
        
        if not filings:
            return {"status": "error", "message": "No 13F filings found"}
        
        latest = filings[0]
        holdings = scraper.get_13f_holdings(cik, la
