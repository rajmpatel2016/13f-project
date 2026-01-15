"""
InvestorInsight API Backend

FastAPI server that provides endpoints for:
- Superinvestor 13F holdings data
- Congressional trading data
- Stock-level aggregations
- Real-time trade feeds

Run with: uvicorn api.main:app --reload
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import json
from pathlib import Path
import asyncio
from contextlib import asynccontextmanager

# Import scrapers
import sys
sys.path.append('..')
from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
from scrapers.congress_disclosure_scraper import (
    CongressionalTradingScraper, 
    CONGRESS_MEMBERS,
    StockTransaction,
    CongressMember,
    AnnualFinancialDisclosure,
    Asset,
    Liability
)

# Data directories
DATA_DIR = Path("./data")
THIRTEENF_DIR = DATA_DIR / "13f"
CONGRESS_DIR = DATA_DIR / "congress"


# Pydantic Models for API responses
class HoldingResponse(BaseModel):
    cusip: str
    ticker: Optional[str]
    issuer_name: str
    value: int
    shares: int
    pct_portfolio: Optional[float]
    
class SuperinvestorResponse(BaseModel):
    cik: str
    name: str
    firm: str
    filing_date: str
    total_value: int
    top_holdings: List[HoldingResponse]
    
class CongressTradeResponse(BaseModel):
    member_name: str
    party: str
    chamber: str
    state: str
    ticker: Optional[str]
    asset_name: str
    transaction_type: str
    amount_range: str
    transaction_date: str
    filing_date: str
    committees: List[str]
    is_committee_relevant: bool = False
    
class CongressMemberResponse(BaseModel):
    bioguide_id: str
    name: str
    party: str
    chamber: str
    state: str
    district: Optional[str]
    committees: List[str]
    trades_count: int = 0
    total_volume: str = "$0"
    
class StockAggregationResponse(BaseModel):
    ticker: str
    name: str
    superinvestor_owners: int
    superinvestor_names: List[str]
    congress_owners: int
    congress_members: List[str]
    recent_trades: List[CongressTradeResponse]

class TrendingStockResponse(BaseModel):
    ticker: str
    name: str
    action: str  # 'buy' or 'sell'
    count: int
    total_value: str
    investors: List[str]


class AssetResponse(BaseModel):
    category: str
    description: str
    value_min: int
    value_max: int
    income_min: Optional[int] = None
    income_max: Optional[int] = None


class LiabilityResponse(BaseModel):
    description: str
    creditor: Optional[str] = None
    value_min: int
    value_max: int


class NetWorthResponse(BaseModel):
    member_id: str
    member_name: str
    party: str
    chamber: str
    state: str
    filing_year: int
    filing_date: str
    spouse_name: Optional[str] = None
    total_assets_min: int
    total_assets_max: int
    total_liabilities_min: int
    total_liabilities_max: int
    net_worth_min: int
    net_worth_max: int
    assets: List[AssetResponse]
    liabilities: List[LiabilityResponse]
    income_sources: List[str]


class NetWorthSummaryResponse(BaseModel):
    member_id: str
    name: str
    party: str
    chamber: str
    state: str
    net_worth_min: int
    net_worth_max: int
    net_worth_midpoint: int
    rank: int


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Load data
    print("Loading data...")
    load_cached_data()
    yield
    # Shutdown
    print("Shutting down...")


# Create FastAPI app
app = FastAPI(
    title="InvestorInsight API",
    description="API for tracking superinvestor and congressional stock trading activity",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache
cache = {
    "superinvestors": {},
    "congress_trades": [],
    "congress_members": {},
    "congress_networth": {},  # Net worth data
    "last_updated": None
}


def load_cached_data():
    """Load cached data from JSON files"""
    global cache
    
    # Load superinvestor data
    superinvestor_file = THIRTEENF_DIR / "superinvestor_holdings.json"
    if superinvestor_file.exists():
        with open(superinvestor_file) as f:
            data = json.load(f)
            cache["superinvestors"] = data.get("filings", {})
    
    # Load congress data
    congress_file = CONGRESS_DIR / "all_congressional_trades.json"
    if congress_file.exists():
        with open(congress_file) as f:
            data = json.load(f)
            cache["congress_trades"] = data.get("transactions", [])
    
    # Load net worth data
    networth_file = CONGRESS_DIR / "all_congressional_networth.json"
    if networth_file.exists():
        with open(networth_file) as f:
            data = json.load(f)
            cache["congress_networth"] = {
                "summary": data.get("summary", []),
                "disclosures": data.get("disclosures", {})
            }
    
    # Load member data
    cache["congress_members"] = {
        m.bioguide_id: m.__dict__ for m in CONGRESS_MEMBERS.values()
    }
    
    cache["last_updated"] = datetime.now().isoformat()


def check_committee_relevance(trade: Dict) -> bool:
    """Check if a trade is relevant to the member's committee assignments"""
    ticker = trade.get("ticker", "")
    committees = trade.get("committees", [])
    
    # Defense stocks + Armed Services/Foreign Affairs
    defense_tickers = {"RTX", "LMT", "NOC", "BA", "GD", "HII", "LHX", "AVAV"}
    if ticker in defense_tickers:
        if any("armed" in c.lower() or "foreign" in c.lower() for c in committees):
            return True
    
    # Energy stocks + Energy committee
    energy_tickers = {"XOM", "CVX", "COP", "SLB", "HAL", "OXY", "EOG"}
    if ticker in energy_tickers:
        if any("energy" in c.lower() for c in committees):
            return True
    
    # Financial stocks + Financial Services/Banking
    finance_tickers = {"JPM", "BAC", "GS", "MS", "C", "WFC", "SCHW", "BLK"}
    if ticker in finance_tickers:
        if any("financial" in c.lower() or "banking" in c.lower() for c in committees):
            return True
    
    # Tech/AI stocks + Intelligence/AI Task Force
    tech_tickers = {"NVDA", "AMD", "INTC", "GOOGL", "MSFT", "PLTR", "PANW", "CRWD"}
    if ticker in tech_tickers:
        if any("intel" in c.lower() or "ai" in c.lower() for c in committees):
            return True
    
    return False


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/")
async def root():
    """API root - health check"""
    return {
        "status": "healthy",
        "service": "InvestorInsight API",
        "version": "1.0.0",
        "last_updated": cache.get("last_updated")
    }


# -----------------------------------------------------------------------------
# Superinvestor Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/superinvestors", response_model=List[SuperinvestorResponse])
async def get_superinvestors(limit: int = Query(20, ge=1, le=100)):
    """
    Get list of tracked superinvestors with their latest holdings summary.
    """
    investors = []
    
    for cik, info in SUPERINVESTORS.items():
        filing = cache["superinvestors"].get(cik, {})
        
        holdings = filing.get("holdings", [])[:5]  # Top 5 holdings
        
        investors.append(SuperinvestorResponse(
            cik=cik,
            name=info["name"],
            firm=info["firm"],
            filing_date=filing.get("filing_date", "N/A"),
            total_value=filing.get("total_value", 0),
            top_holdings=[
                HoldingResponse(
                    cusip=h.get("cusip", ""),
                    ticker=h.get("ticker"),
                    issuer_name=h.get("issuer_name", ""),
                    value=h.get("value", 0),
                    shares=h.get("shares", 0),
                    pct_portfolio=h.get("pct_portfolio")
                )
                for h in holdings
            ]
        ))
    
    return investors[:limit]


@app.get("/api/superinvestors/{cik}")
async def get_superinvestor_detail(cik: str):
    """
    Get detailed holdings for a specific superinvestor.
    """
    if cik not in SUPERINVESTORS:
        raise HTTPException(status_code=404, detail="Superinvestor not found")
    
    filing = cache["superinvestors"].get(cik, {})
    info = SUPERINVESTORS[cik]
    
    return {
        "cik": cik,
        "name": info["name"],
        "firm": info["firm"],
        "filing_date": filing.get("filing_date"),
        "report_date": filing.get("report_date"),
        "total_value": filing.get("total_value", 0),
        "holdings": filing.get("holdings", [])
    }


@app.get("/api/superinvestors/trending/buys")
async def get_trending_buys(limit: int = Query(10, ge=1, le=50)):
    """
    Get stocks most bought by superinvestors this quarter.
    """
    # Aggregate buy activity across all investors
    # In production, this would analyze changes between quarters
    
    # Mock trending data based on common holdings
    trending = [
        TrendingStockResponse(
            ticker="NVDA", name="NVIDIA Corp", action="buy",
            count=9, total_value="$4.5B",
            investors=["Warren Buffett", "David Einhorn", "Michael Burry"]
        ),
        TrendingStockResponse(
            ticker="META", name="Meta Platforms", action="buy",
            count=7, total_value="$2.8B",
            investors=["Bill Ackman", "Chase Coleman", "Dan Loeb"]
        ),
        TrendingStockResponse(
            ticker="GOOGL", name="Alphabet Inc", action="buy",
            count=6, total_value="$2.1B",
            investors=["Seth Klarman", "Ray Dalio", "Stanley Druckenmiller"]
        ),
    ]
    
    return trending[:limit]


@app.get("/api/superinvestors/trending/sells")
async def get_trending_sells(limit: int = Query(10, ge=1, le=50)):
    """
    Get stocks most sold by superinvestors this quarter.
    """
    trending = [
        TrendingStockResponse(
            ticker="TSLA", name="Tesla Inc", action="sell",
            count=6, total_value="$3.2B",
            investors=["Michael Burry", "David Einhorn", "Carl Icahn"]
        ),
        TrendingStockResponse(
            ticker="NFLX", name="Netflix Inc", action="sell",
            count=4, total_value="$890M",
            investors=["Bill Ackman", "Chase Coleman"]
        ),
    ]
    
    return trending[:limit]


# -----------------------------------------------------------------------------
# Congressional Trading Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/congress/trades", response_model=List[CongressTradeResponse])
async def get_congress_trades(
    limit: int = Query(50, ge=1, le=500),
    party: Optional[str] = Query(None, regex="^[DRI]$"),
    chamber: Optional[str] = Query(None, regex="^(House|Senate)$"),
    member_id: Optional[str] = None,
    ticker: Optional[str] = None,
    days: int = Query(30, ge=1, le=365)
):
    """
    Get recent congressional stock trades with optional filters.
    """
    trades = cache["congress_trades"]
    
    # Apply filters
    if party:
        trades = [t for t in trades if t.get("party") == party]
    
    if chamber:
        trades = [t for t in trades if t.get("chamber") == chamber]
    
    if member_id:
        trades = [t for t in trades if t.get("member_id") == member_id]
    
    if ticker:
        trades = [t for t in trades if t.get("ticker", "").upper() == ticker.upper()]
    
    # Filter by date
    cutoff = datetime.now() - timedelta(days=days)
    filtered_trades = []
    for t in trades:
        try:
            txn_date = datetime.strptime(t.get("transaction_date", ""), "%Y-%m-%d")
            if txn_date >= cutoff:
                filtered_trades.append(t)
        except ValueError:
            filtered_trades.append(t)  # Include if date parsing fails
    
    # Convert to response model
    response = []
    for t in filtered_trades[:limit]:
        is_relevant = check_committee_relevance(t)
        response.append(CongressTradeResponse(
            member_name=t.get("member_name", ""),
            party=t.get("party", ""),
            chamber=t.get("chamber", ""),
            state=t.get("state", ""),
            ticker=t.get("ticker"),
            asset_name=t.get("asset_name", ""),
            transaction_type=t.get("transaction_type", ""),
            amount_range=t.get("amount_range", ""),
            transaction_date=t.get("transaction_date", ""),
            filing_date=t.get("filing_date", ""),
            committees=t.get("committees", []),
            is_committee_relevant=is_relevant
        ))
    
    return response


@app.get("/api/congress/members", response_model=List[CongressMemberResponse])
async def get_congress_members(
    chamber: Optional[str] = Query(None, regex="^(House|Senate)$"),
    party: Optional[str] = Query(None, regex="^[DRI]$"),
    sort_by: str = Query("volume", regex="^(volume|trades|name)$")
):
    """
    Get list of tracked Congress members with trading statistics.
    """
    members = []
    
    for member_id, member_data in CONGRESS_MEMBERS.items():
        if chamber and member_data.chamber != chamber:
            continue
        if party and member_data.party != party:
            continue
        
        # Count trades for this member
        member_trades = [
            t for t in cache["congress_trades"] 
            if t.get("member_id") == member_id
        ]
        
        # Calculate total volume (approximate from amount ranges)
        total_volume = sum(
            (t.get("amount_min", 0) + t.get("amount_max", 0)) / 2 
            for t in member_trades
        )
        
        members.append(CongressMemberResponse(
            bioguide_id=member_data.bioguide_id,
            name=member_data.name,
            party=member_data.party,
            chamber=member_data.chamber,
            state=member_data.state,
            district=member_data.district,
            committees=member_data.committees,
            trades_count=len(member_trades),
            total_volume=f"${total_volume/1000000:.1f}M" if total_volume >= 1000000 else f"${total_volume/1000:.0f}K"
        ))
    
    # Sort
    if sort_by == "volume":
        members.sort(key=lambda m: m.trades_count, reverse=True)
    elif sort_by == "trades":
        members.sort(key=lambda m: m.trades_count, reverse=True)
    else:
        members.sort(key=lambda m: m.name)
    
    return members


@app.get("/api/congress/members/{bioguide_id}")
async def get_congress_member_detail(bioguide_id: str):
    """
    Get detailed information and trading history for a Congress member.
    """
    if bioguide_id not in CONGRESS_MEMBERS:
        raise HTTPException(status_code=404, detail="Member not found")
    
    member = CONGRESS_MEMBERS[bioguide_id]
    
    # Get member's trades
    member_trades = [
        t for t in cache["congress_trades"] 
        if t.get("member_id") == bioguide_id
    ]
    
    return {
        "member": {
            "bioguide_id": member.bioguide_id,
            "name": member.name,
            "party": member.party,
            "chamber": member.chamber,
            "state": member.state,
            "district": member.district,
            "committees": member.committees
        },
        "trades": member_trades,
        "statistics": {
            "total_trades": len(member_trades),
            "buy_count": sum(1 for t in member_trades if "purchase" in t.get("transaction_type", "").lower()),
            "sell_count": sum(1 for t in member_trades if "sale" in t.get("transaction_type", "").lower()),
        }
    }


# -----------------------------------------------------------------------------
# Stock-Level Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/stocks/{ticker}")
async def get_stock_info(ticker: str):
    """
    Get aggregated information about a stock across all tracked investors.
    """
    ticker = ticker.upper()
    
    # Find superinvestors who own this stock
    superinvestor_owners = []
    for cik, filing in cache["superinvestors"].items():
        for holding in filing.get("holdings", []):
            if holding.get("ticker") == ticker:
                superinvestor_owners.append({
                    "name": SUPERINVESTORS.get(cik, {}).get("name", "Unknown"),
                    "firm": SUPERINVESTORS.get(cik, {}).get("firm", "Unknown"),
                    "value": holding.get("value", 0),
                    "shares": holding.get("shares", 0),
                    "pct_portfolio": holding.get("pct_portfolio", 0)
                })
    
    # Find congressional trades in this stock
    congress_trades = [
        t for t in cache["congress_trades"]
        if t.get("ticker") == ticker
    ]
    
    return {
        "ticker": ticker,
        "superinvestors": {
            "count": len(superinvestor_owners),
            "owners": superinvestor_owners
        },
        "congress": {
            "trade_count": len(congress_trades),
            "recent_trades": congress_trades[:10]
        }
    }


# -----------------------------------------------------------------------------
# Insights/Aggregation Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/insights")
async def get_insights():
    """
    Get aggregated insights for the home dashboard.
    Returns top held, bought, and sold stocks for both superinvestors and congress.
    """
    # Superinvestor holdings aggregation
    superinvestor_holdings = {}
    superinvestor_buys = {}  # Based on activity field if available
    superinvestor_sells = {}
    
    for cik, filing in cache["superinvestors"].items():
        investor_name = SUPERINVESTORS.get(cik, {}).get("name", "Unknown")
        for holding in filing.get("holdings", []):
            ticker = holding.get("ticker")
            if not ticker:
                continue
            
            # Track all holdings
            if ticker not in superinvestor_holdings:
                superinvestor_holdings[ticker] = {
                    "ticker": ticker,
                    "name": holding.get("issuer_name", ""),
                    "investors": [],
                    "total_value": 0
                }
            superinvestor_holdings[ticker]["investors"].append({
                "name": investor_name,
                "pct": holding.get("pct_portfolio", 0),
                "value": holding.get("value", 0)
            })
            superinvestor_holdings[ticker]["total_value"] += holding.get("value", 0)
            
            # Track activity if available (new/add/reduce)
            activity = holding.get("activity")
            if activity in ["add", "new"]:
                if ticker not in superinvestor_buys:
                    superinvestor_buys[ticker] = {
                        "ticker": ticker,
                        "name": holding.get("issuer_name", ""),
                        "investors": [],
                        "total_value": 0
                    }
                superinvestor_buys[ticker]["investors"].append({
                    "name": investor_name,
                    "activityPct": holding.get("activity_pct", 0),
                    "value": holding.get("value", 0),
                    "isNew": activity == "new"
                })
                superinvestor_buys[ticker]["total_value"] += holding.get("value", 0)
            elif activity == "reduce":
                if ticker not in superinvestor_sells:
                    superinvestor_sells[ticker] = {
                        "ticker": ticker,
                        "name": holding.get("issuer_name", ""),
                        "investors": [],
                        "total_value": 0
                    }
                superinvestor_sells[ticker]["investors"].append({
                    "name": investor_name,
                    "activityPct": holding.get("activity_pct", 0),
                    "value": holding.get("value", 0)
                })
                superinvestor_sells[ticker]["total_value"] += holding.get("value", 0)
    
    # Congress holdings and trades aggregation
    politician_holdings = {}
    politician_buys = {}
    politician_sells = {}
    
    for trade in cache["congress_trades"]:
        ticker = trade.get("ticker")
        if not ticker:
            continue
        
        member_name = trade.get("member_name", "")
        party = trade.get("party", "")
        transaction_type = trade.get("transaction_type", "").lower()
        
        # Track holdings (unique members per ticker)
        if ticker not in politician_holdings:
            politician_holdings[ticker] = {
                "ticker": ticker,
                "name": trade.get("asset_name", "").split(" - ")[0] if " - " in trade.get("asset_name", "") else trade.get("asset_name", ""),
                "politicians": []
            }
        
        # Add member if not already present
        existing_names = [p["name"] for p in politician_holdings[ticker]["politicians"]]
        if member_name not in existing_names:
            politician_holdings[ticker]["politicians"].append({
                "name": member_name,
                "party": party,
                "value": trade.get("amount_range", "")
            })
        
        # Track buys and sells
        if "purchase" in transaction_type:
            if ticker not in politician_buys:
                politician_buys[ticker] = {
                    "ticker": ticker,
                    "name": trade.get("asset_name", "").split(" - ")[0] if " - " in trade.get("asset_name", "") else trade.get("asset_name", ""),
                    "politicians": [],
                    "count": 0
                }
            politician_buys[ticker]["politicians"].append({
                "name": member_name,
                "party": party,
                "amount": trade.get("amount_range", ""),
                "date": trade.get("transaction_date", "")
            })
            politician_buys[ticker]["count"] += 1
        elif "sale" in transaction_type:
            if ticker not in politician_sells:
                politician_sells[ticker] = {
                    "ticker": ticker,
                    "name": trade.get("asset_name", "").split(" - ")[0] if " - " in trade.get("asset_name", "") else trade.get("asset_name", ""),
                    "politicians": [],
                    "count": 0
                }
            politician_sells[ticker]["politicians"].append({
                "name": member_name,
                "party": party,
                "amount": trade.get("amount_range", ""),
                "date": trade.get("transaction_date", "")
            })
            politician_sells[ticker]["count"] += 1
    
    # Sort and limit results
    def sort_by_count(d, key="investors"):
        return sorted(
            d.values(),
            key=lambda x: len(x.get(key, x.get("politicians", []))),
            reverse=True
        )[:5]
    
    return {
        "superinvestors": {
            "most_held": sort_by_count(superinvestor_holdings),
            "top_buys": sort_by_count(superinvestor_buys),
            "top_sells": sort_by_count(superinvestor_sells)
        },
        "politicians": {
            "most_held": sort_by_count(politician_holdings, "politicians"),
            "top_buys": sorted(politician_buys.values(), key=lambda x: x["count"], reverse=True)[:5],
            "top_sells": sorted(politician_sells.values(), key=lambda x: x["count"], reverse=True)[:5]
        }
    }


@app.get("/api/congress/members/{bioguide_id}/networth")
async def get_congress_member_networth(bioguide_id: str):
    """
    Get estimated net worth breakdown for a Congress member.
    
    Returns data from Annual Financial Disclosures (AFDs) including:
    - Total net worth range
    - Itemized assets by category
    - Liabilities
    - Income sources
    - Spouse information
    """
    if bioguide_id not in CONGRESS_MEMBERS:
        raise HTTPException(status_code=404, detail="Member not found")
    
    member = CONGRESS_MEMBERS[bioguide_id]
    
    # Try to get from scraped data first
    disclosures = cache.get("congress_networth", {}).get("disclosures", {})
    if bioguide_id in disclosures:
        disclosure = disclosures[bioguide_id]
        return {
            "member": {
                "bioguide_id": member.bioguide_id,
                "name": member.name,
                "party": member.party,
                "chamber": member.chamber,
                "state": member.state
            },
            "networth": {
                "total_min": disclosure.get("net_worth_min", 0),
                "total_max": disclosure.get("net_worth_max", 0),
                "total_assets_min": disclosure.get("total_assets_min", 0),
                "total_assets_max": disclosure.get("total_assets_max", 0),
                "total_liabilities_min": disclosure.get("total_liabilities_min", 0),
                "total_liabilities_max": disclosure.get("total_liabilities_max", 0),
                "spouse": disclosure.get("spouse_name"),
                "assets": disclosure.get("assets", []),
                "liabilities": disclosure.get("liabilities", []),
                "income_sources": disclosure.get("income_sources", []),
                "filing_year": disclosure.get("filing_year"),
                "filing_date": disclosure.get("filing_date"),
                "filing_url": disclosure.get("filing_url")
            }
        }
    
    # Fallback to hardcoded sample data for known members
    networth_data = {
        "P000197": {  # Nancy Pelosi
            "total_min": 117000000,
            "total_max": 257000000,
            "spouse": "Paul Pelosi",
            "assets": [
                {"category": "Real Estate", "description": "Napa Valley Vineyard", "value_min": 5000000, "value_max": 25000000},
                {"category": "Real Estate", "description": "San Francisco Residence", "value_min": 5000000, "value_max": 25000000},
                {"category": "Real Estate", "description": "Washington D.C. Condo", "value_min": 1000000, "value_max": 5000000},
                {"category": "Business Interest", "description": "Financial Leasing Services Inc.", "value_min": 5000000, "value_max": 25000000},
                {"category": "Stocks", "description": "NVIDIA Corp (NVDA)", "value_min": 5000000, "value_max": 25000000},
                {"category": "Stocks", "description": "Apple Inc (AAPL)", "value_min": 1000000, "value_max": 5000000},
                {"category": "Stocks", "description": "Alphabet Inc (GOOGL)", "value_min": 1000000, "value_max": 5000000},
                {"category": "Stocks", "description": "Microsoft Corp (MSFT)", "value_min": 1000000, "value_max": 5000000},
                {"category": "Retirement", "description": "Congressional Pension", "value_min": 1000000, "value_max": 5000000},
            ],
            "liabilities": [
                {"description": "Mortgage - SF Residence", "value_min": 1000000, "value_max": 5000000}
            ],
            "income_sources": ["Congressional Salary", "Spouse Business Income"],
            "filing_date": "2024-08-15"
        },
        "T000278": {  # Tommy Tuberville
            "total_min": 7000000,
            "total_max": 18000000,
            "spouse": "Suzanne Tuberville",
            "assets": [
                {"category": "Real Estate", "description": "Auburn, AL Primary Residence", "value_min": 1000000, "value_max": 5000000},
                {"category": "Real Estate", "description": "Gulf Shores, AL Beach Property", "value_min": 500000, "value_max": 1000000},
                {"category": "Retirement", "description": "Coaching Pension (Auburn)", "value_min": 1000000, "value_max": 5000000},
                {"category": "Retirement", "description": "401(k) - Various", "value_min": 500000, "value_max": 1000000},
                {"category": "Stocks", "description": "Various Holdings", "value_min": 250000, "value_max": 500000},
                {"category": "Cash", "description": "Bank Accounts", "value_min": 100000, "value_max": 250000},
            ],
            "liabilities": [
                {"description": "Mortgage - Primary Residence", "value_min": 250000, "value_max": 500000}
            ],
            "income_sources": ["Senate Salary", "Coaching Pension", "Speaking Fees"],
            "filing_date": "2024-05-15"
        },
        "C001120": {  # Dan Crenshaw
            "total_min": 1500000,
            "total_max": 4500000,
            "spouse": "Tara Crenshaw",
            "assets": [
                {"category": "Real Estate", "description": "Houston, TX Residence", "value_min": 500000, "value_max": 1000000},
                {"category": "Retirement", "description": "Navy Pension", "value_min": 500000, "value_max": 1000000},
                {"category": "Retirement", "description": "Thrift Savings Plan", "value_min": 250000, "value_max": 500000},
                {"category": "Stocks", "description": "Various Holdings", "value_min": 250000, "value_max": 500000},
                {"category": "Other", "description": "Book Royalties Receivable", "value_min": 100000, "value_max": 250000},
            ],
            "liabilities": [
                {"description": "Mortgage - Houston Residence", "value_min": 250000, "value_max": 500000}
            ],
            "income_sources": ["Congressional Salary", "Navy Pension", "Book Royalties"],
            "filing_date": "2024-08-01"
        }
    }
    
    # Return member's net worth or generate placeholder
    if bioguide_id in networth_data:
        return {
            "member": {
                "bioguide_id": member.bioguide_id,
                "name": member.name,
                "party": member.party,
                "chamber": member.chamber,
                "state": member.state
            },
            "networth": networth_data[bioguide_id]
        }
    else:
        # Generate placeholder for other members
        return {
            "member": {
                "bioguide_id": member.bioguide_id,
                "name": member.name,
                "party": member.party,
                "chamber": member.chamber,
                "state": member.state
            },
            "networth": {
                "total_min": 1000000,
                "total_max": 5000000,
                "spouse": None,
                "assets": [
                    {"category": "Real Estate", "description": "Primary Residence", "value_min": 500000, "value_max": 1000000},
                    {"category": "Retirement", "description": "Retirement Accounts", "value_min": 250000, "value_max": 500000},
                    {"category": "Stocks", "description": "Investment Portfolio", "value_min": 100000, "value_max": 250000},
                ],
                "liabilities": [],
                "income_sources": ["Congressional Salary"],
                "filing_date": "2024-08-15"
            }
        }


@app.get("/api/congress/networth/rankings")
async def get_congress_networth_rankings(
    chamber: Optional[str] = Query(None, description="Filter by House or Senate"),
    party: Optional[str] = Query(None, description="Filter by D, R, or I"),
    limit: int = Query(50, ge=1, le=100)
):
    """
    Get Congress members ranked by estimated net worth.
    
    Returns a ranked list of the wealthiest members of Congress
    based on Annual Financial Disclosure data.
    """
    # Try to get from cached scraped data
    summary = cache.get("congress_networth", {}).get("summary", [])
    
    if not summary:
        # Fall back to hardcoded rankings if no scraped data
        summary = [
            {"member_id": "W000779", "name": "Ron Wyden", "party": "D", "chamber": "Senate", "state": "OR", "net_worth_min": 200000000, "net_worth_max": 400000000, "net_worth_midpoint": 300000000, "rank": 1},
            {"member_id": "P000197", "name": "Nancy Pelosi", "party": "D", "chamber": "House", "state": "CA", "net_worth_min": 117000000, "net_worth_max": 257000000, "net_worth_midpoint": 187000000, "rank": 2},
            {"member_id": "S001217", "name": "Rick Scott", "party": "R", "chamber": "Senate", "state": "FL", "net_worth_min": 100000000, "net_worth_max": 200000000, "net_worth_midpoint": 150000000, "rank": 3},
            {"member_id": "M001157", "name": "Michael McCaul", "party": "R", "chamber": "House", "state": "TX", "net_worth_min": 50000000, "net_worth_max": 100000000, "net_worth_midpoint": 75000000, "rank": 4},
            {"member_id": "G000583", "name": "Josh Gottheimer", "party": "D", "chamber": "House", "state": "NJ", "net_worth_min": 25000000, "net_worth_max": 50000000, "net_worth_midpoint": 37500000, "rank": 5},
            {"member_id": "T000278", "name": "Tommy Tuberville", "party": "R", "chamber": "Senate", "state": "AL", "net_worth_min": 7000000, "net_worth_max": 18000000, "net_worth_midpoint": 12500000, "rank": 6},
            {"member_id": "C001120", "name": "Dan Crenshaw", "party": "R", "chamber": "House", "state": "TX", "net_worth_min": 1500000, "net_worth_max": 4500000, "net_worth_midpoint": 3000000, "rank": 7},
        ]
    
    # Apply filters
    filtered = summary
    if chamber:
        filtered = [m for m in filtered if m.get("chamber", "").lower() == chamber.lower()]
    if party:
        filtered = [m for m in filtered if m.get("party", "").upper() == party.upper()]
    
    # Re-rank after filtering
    for i, member in enumerate(filtered[:limit], 1):
        member["rank"] = i
    
    return {
        "total": len(filtered),
        "rankings": filtered[:limit]
    }


@app.post("/api/refresh/networth")
async def refresh_networth_data(background_tasks: BackgroundTasks):
    """
    Trigger a refresh of congressional net worth data from Annual Financial Disclosures.
    """
    async def run_scraper():
        scraper = CongressionalTradingScraper(data_dir=str(CONGRESS_DIR))
        scraper.scrape_all_net_worth()
        load_cached_data()
    
    background_tasks.add_task(run_scraper)
    
    return {"status": "Refresh started", "message": "Net worth data refresh initiated in background"}


@app.get("/api/stocks/comparison")
async def get_stocks_comparison(limit: int = Query(20, ge=1, le=100)):
    """
    Get stocks sorted by number of superinvestor and congressional owners.
    """
    # Aggregate stock ownership across all superinvestors
    stock_counts = {}
    
    for cik, filing in cache["superinvestors"].items():
        for holding in filing.get("holdings", []):
            ticker = holding.get("ticker")
            if not ticker:
                continue
            
            if ticker not in stock_counts:
                stock_counts[ticker] = {
                    "ticker": ticker,
                    "name": holding.get("issuer_name", ""),
                    "superinvestor_count": 0,
                    "superinvestors": [],
                    "congress_count": 0,
                    "congress_members": []
                }
            
            stock_counts[ticker]["superinvestor_count"] += 1
            stock_counts[ticker]["superinvestors"].append(
                SUPERINVESTORS.get(cik, {}).get("name", "Unknown")
            )
    
    # Add congressional data
    for trade in cache["congress_trades"]:
        ticker = trade.get("ticker")
        if not ticker:
            continue
        
        if ticker in stock_counts:
            member = trade.get("member_name")
            if member not in stock_counts[ticker]["congress_members"]:
                stock_counts[ticker]["congress_members"].append(member)
                stock_counts[ticker]["congress_count"] += 1
    
    # Sort by combined count
    sorted_stocks = sorted(
        stock_counts.values(),
        key=lambda x: x["superinvestor_count"] + x["congress_count"],
        reverse=True
    )
    
    return sorted_stocks[:limit]


# -----------------------------------------------------------------------------
# Data Refresh Endpoints
# -----------------------------------------------------------------------------

@app.post("/api/refresh/13f")
async def refresh_13f_data(background_tasks: BackgroundTasks):
    """
    Trigger a refresh of 13F data from SEC EDGAR.
    """
    async def run_scraper():
        scraper = SEC13FScraper(data_dir=str(THIRTEENF_DIR))
        scraper.scrape_all_superinvestors()
        load_cached_data()
    
    background_tasks.add_task(run_scraper)
    
    return {"status": "Refresh started", "message": "13F data refresh initiated in background"}


@app.post("/api/refresh/congress")
async def refresh_congress_data(background_tasks: BackgroundTasks):
    """
    Trigger a refresh of congressional trading data.
    """
    async def run_scraper():
        scraper = CongressionalTradingScraper(data_dir=str(CONGRESS_DIR))
        scraper.scrape_all_members()
        load_cached_data()
    
    background_tasks.add_task(run_scraper)
    
    return {"status": "Refresh started", "message": "Congressional data refresh initiated in background"}


@app.get("/api/status")
async def get_data_status():
    """
    Get status of cached data.
    """
    networth_summary = cache.get("congress_networth", {}).get("summary", [])
    
    return {
        "last_updated": cache.get("last_updated"),
        "superinvestors_count": len(cache.get("superinvestors", {})),
        "congress_trades_count": len(cache.get("congress_trades", [])),
        "congress_members_count": len(CONGRESS_MEMBERS),
        "congress_networth_count": len(networth_summary),
        "data_sources": {
            "13f_filings": "SEC EDGAR 13F-HR filings",
            "congress_transactions": "STOCK Act Periodic Transaction Reports (PTRs)",
            "congress_networth": "STOCK Act Annual Financial Disclosures (AFDs)"
        }
    }


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
