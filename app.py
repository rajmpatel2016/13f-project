from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import re
import xml.etree.ElementTree as ET

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"name": "InvestorInsight API", "status": "online", "version": "v4"}

@app.get("/api/debug-parse")
def debug_parse():
    """Test XML parsing directly"""
    try:
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        xml_url = "https://www.sec.gov/Archives/edgar/data/1649339/000164933925000007/infotable.xml"
        
        r = requests.get(xml_url, headers=headers, timeout=15)
        xml_content = r.text
        
        # Remove namespaces
        xml_clean = re.sub(r'\sxmlns[^=]*="[^"]*"', '', xml_content)
        xml_clean = re.sub(r'<(/?)(\w+):', r'<\1', xml_clean)
        
        root = ET.fromstring(xml_clean)
        
        # Find all tags
        all_tags = [elem.tag for elem in root.iter()]
        
        # Try to find infoTable entries
        holdings = []
        for elem in root.iter():
            if elem.tag.lower() == 'infotable':
                # Try to extract data
                cusip = None
                name = None
                value = None
                for child in elem.iter():
                    if 'cusip' in child.tag.lower():
                        cusip = child.text
                    if 'nameofissuer' in child.tag.lower():
                        name = child.text
                    if child.tag.lower() == 'value':
                        value = child.text
                if cusip:
                    holdings.append({"cusip": cusip, "name": name, "value": value})
        
        return {
            "status": "ok",
            "total_tags": len(all_tags),
            "unique_tags": list(set(all_tags)),
            "holdings_found": len(holdings),
            "first_3_holdings": holdings[:3]
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
        holdings = scraper.get_13f_holdings(cik, latest["accession_number"])
        
        if not holdings:
            return {
                "status": "partial",
                "message": "Found filing but couldn't parse holdings",
                "filing": latest
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
