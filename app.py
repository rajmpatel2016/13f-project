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
    return {"name": "InvestorInsight API", "status": "online"}

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
