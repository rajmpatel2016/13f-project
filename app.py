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
    return {"name": "InvestorInsight API", "status": "online", "version": "v7"}

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
            return {"status": "partial", "message": "Found filing but couldn't parse holdings", "filing": latest}
        
        total_value = sum(h.value for h in holdings)
        for h in holdings:
            if total_value > 0:
                h.pct_portfolio = round((h.value / total_value) * 100, 2)
        
        holdings.sort(key=lambda x: x.value, reverse=True)
        
        return {
            "status": "success",
            "investor": SUPERINVESTORS[cik]["name"],
            "firm": SUPERINVESTORS[cik]["firm"],
            "filing_date": latest["filing_date"],
            "total_value": total_value,
            "positions": len(holdings),
            "top_5": [
                {"ticker": h.ticker or h.cusip[:6], "name": h.issuer_name, "value": h.value, "pct": h.pct_portfolio}
                for h in holdings[:5]
            ]
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}
