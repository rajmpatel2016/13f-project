from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "healthy"}

@app.get("/api/superinvestors")
def get_superinvestors():
    from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
    
    scraper = SEC13FScraper(data_dir="./data/13f")
    results = []
    
    for cik, info in SUPERINVESTORS.items():
        filings = scraper.get_cik_filings(cik, "13F-HR")
        if filings:
            holdings = scraper.get_13f_holdings(cik, filings[0]["accession_number"])
            total_value = sum(h.value for h in holdings) if holdings else 0
            results.append({
                "cik": cik,
                "name": info["name"],
                "firm": info["firm"],
                "value": total_value,
                "filing_date": filings[0]["filing_date"]
            })
    
    results.sort(key=lambda x: x["value"], reverse=True)
    return results

@app.get("/api/superinvestors/{cik}")
def get_superinvestor(cik: str):
    from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
    
    if cik not in SUPERINVESTORS:
        return {"error": "Not found"}
    
    scraper = SEC13FScraper(data_dir="./data/13f")
    info = SUPERINVESTORS[cik]
    filings = scraper.get_cik_filings(cik, "13F-HR")
    
    if not filings:
        return {"error": "No filings"}
    
    holdings = scraper.get_13f_holdings(cik, filings[0]["accession_number"])
    
    if not holdings:
        return {"error": "No holdings"}
    
    total_value = sum(h.value for h in holdings)
    for h in holdings:
        if total_value > 0:
            h.pct_portfolio = round((h.value / total_value) * 100, 2)
    
    holdings.sort(key=lambda x: x.value, reverse=True)
    
    return {
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
