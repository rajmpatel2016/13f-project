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
    return {"name": "InvestorInsight API", "status": "online", "version": "v6"}

@app.get("/api/debug-parse")
def debug_parse():
    """Parse XML with regex instead of ElementTree"""
    try:
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        xml_url = "https://www.sec.gov/Archives/edgar/data/1649339/000164933925000007/infotable.xml"
        
        r = requests.get(xml_url, headers=headers, timeout=15)
        xml_content = r.text
        
        # Use regex to extract holdings directly
        holdings = []
        
        # Find all infoTable blocks
        info_tables = re.findall(r'<infoTable>(.*?)</infoTable>', xml_content, re.DOTALL)
        
        for table in info_tables:
            cusip = re.search(r'<cusip>([^<]+)</cusip>', table)
            name = re.search(r'<nameOfIssuer>([^<]+)</nameOfIssuer>', table)
            value = re.search(r'<value>([^<]+)</value>', table)
            shares = re.search(r'<sshPrnamt>([^<]+)</sshPrnamt>', table)
            
            if cusip:
                holdings.append({
                    "cusip": cusip.group(1),
                    "name": name.group(1) if name else None,
                    "value": int(value.group(1)) if value else 0,
                    "shares": int(shares.group(1)) if shares else 0
                })
        
        return {
            "status": "ok",
            "holdings_found": len(holdings),
            "holdings": holdings
        }
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}
