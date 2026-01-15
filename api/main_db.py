"""
InvestorInsight API Backend - Database Version

FastAPI server using SQLite database for:
- Superinvestor 13F holdings data
- Congressional trading data  
- Stock-level aggregations

Run with: uvicorn api.main:app --reload
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import os
import sys

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db, get_session
from scrapers.sec_13f_scraper import SEC13FScraper, SUPERINVESTORS
from database.models import (
    Superinvestor, Filing13F, Holding,
    CongressMember, CongressTrade, NetWorthReport, NetWorthAsset, NetWorthLiability
)
from sqlalchemy import func, desc, and_
from sqlalchemy.orm import Session


# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class HoldingResponse(BaseModel):
    ticker: Optional[str]
    cusip: Optional[str]
    issuer_name: Optional[str]
    value: int
    shares: int
    pct_portfolio: Optional[float]
    shares_change: Optional[int] = None
    shares_change_pct: Optional[float] = None
    is_new: bool = False
    is_sold: bool = False

class SuperinvestorListItem(BaseModel):
    cik: str
    name: str
    firm: Optional[str]
    total_value: Optional[int]
    filing_date: Optional[str]
    holdings_count: Optional[int]

class SuperinvestorDetail(BaseModel):
    cik: str
    name: str
    firm: Optional[str]
    filing_date: Optional[str]
    report_date: Optional[str]
    total_value: Optional[int]
    holdings: List[HoldingResponse]

class CongressMemberListItem(BaseModel):
    bioguide_id: str
    name: str
    party: Optional[str]
    chamber: Optional[str]
    state: Optional[str]
    trades_count: int = 0

class CongressTradeResponse(BaseModel):
    id: int
    member_name: str
    party: Optional[str]
    chamber: Optional[str]
    state: Optional[str]
    ticker: Optional[str]
    asset_name: Optional[str]
    transaction_type: Optional[str]
    amount_range: Optional[str]
    transaction_date: Optional[str]
    disclosure_date: Optional[str]

class NetWorthResponse(BaseModel):
    member_name: str
    party: Optional[str]
    chamber: Optional[str]
    state: Optional[str]
    report_year: int
    net_worth_min: Optional[int]
    net_worth_max: Optional[int]
    total_assets_min: Optional[int]
    total_assets_max: Optional[int]
    total_liabilities_min: Optional[int]
    total_liabilities_max: Optional[int]
    spouse_name: Optional[str]

class StockHoldersResponse(BaseModel):
    ticker: str
    superinvestor_holders: List[Dict[str, Any]]
    congress_holders: List[Dict[str, Any]]
    recent_congress_trades: List[CongressTradeResponse]

class InsightsResponse(BaseModel):
    top_superinvestor_holdings: List[Dict[str, Any]]
    top_superinvestor_buys: List[Dict[str, Any]]
    top_superinvestor_sells: List[Dict[str, Any]]
    top_congress_holdings: List[Dict[str, Any]]
    top_congress_buys: List[Dict[str, Any]]
    top_congress_sells: List[Dict[str, Any]]


# ═══════════════════════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup"""
    print("Initializing database...")
    init_db()
    print("Database ready!")
    yield
    print("Shutting down...")


app = FastAPI(
    title="InvestorInsight API",
    description="Track superinvestor and congressional stock trading",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    """Database session dependency"""
    db = get_session()
    try:
        yield db
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {
        "name": "InvestorInsight API",
        "version": "2.0.0",
        "status": "online",
        "database": "sqlite"
    }


@app.get("/api/health")
async def health_check(db: Session = Depends(get_db)):
    """Check database connectivity and return stats"""
    try:
        stats = {
            "superinvestors": db.query(Superinvestor).count(),
            "filings": db.query(Filing13F).count(),
            "holdings": db.query(Holding).count(),
            "congress_members": db.query(CongressMember).count(),
            "congress_trades": db.query(CongressTrade).count(),
            "net_worth_reports": db.query(NetWorthReport).count(),
        }
        return {"status": "healthy", "database": "connected", "stats": stats}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/api/test-scraper")
async def test_scraper(cik: str = Query("1067983", description="CIK to test (default: Warren Buffett)")):
    """
    Test the SEC 13F scraper by fetching a single investor's holdings.
    Default: Warren Buffett (CIK: 1067983)
    """
    try:
        # Check if CIK is in our tracked list
        if cik not in SUPERINVESTORS:
            available_ciks = [
                {"cik": k, "name": v["name"], "firm": v["firm"]} 
                for k, v in list(SUPERINVESTORS.items())[:10]
            ]
            return {
                "status": "error",
                "message": f"CIK {cik} not in tracked superinvestors",
                "available_ciks_sample": available_ciks
            }
        
        investor_info = SUPERINVESTORS[cik]
        scraper = SEC13FScraper(data_dir="./data/13f")
        
        # Scrape the investor
        filing = scraper.scrape_investor(cik, investor_info)
        
        if not filing:
            return {
                "status": "error",
                "message": f"No 13F filing found for {investor_info['name']}",
                "cik": cik
            }
        
        # Return top 10 holdings
        top_holdings = [
            {
                "ticker": h.ticker or h.cusip[:6],
                "issuer": h.issuer_name,
                "value_thousands": h.value,
                "shares": h.shares,
                "pct_portfolio": h.pct_portfolio
            }
            for h in filing.holdings[:10]
        ]
        
        return {
            "status": "success",
            "investor": filing.investor_name,
            "firm": filing.firm_name,
            "cik": filing.cik,
            "filing_date": filing.filing_date,
            "report_date": filing.report_date,
            "total_value_thousands": filing.total_value,
            "total_positions": len(filing.holdings),
            "top_10_holdings": top_holdings
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "cik": cik
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SUPERINVESTOR ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/superinvestors", response_model=List[SuperinvestorListItem])
async def get_superinvestors(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get all superinvestors sorted by portfolio value"""
    
    # Subquery to get latest filing for each superinvestor
    latest_filing_subq = db.query(
        Filing13F.superinvestor_id,
        func.max(Filing13F.id).label('max_id')
    ).group_by(Filing13F.superinvestor_id).subquery()
    
    # Join to get superinvestors with their latest filing info
    results = db.query(
        Superinvestor,
        Filing13F.total_value,
        Filing13F.filing_date,
        Filing13F.positions_count
    ).outerjoin(
        latest_filing_subq,
        Superinvestor.id == latest_filing_subq.c.superinvestor_id
    ).outerjoin(
        Filing13F,
        Filing13F.id == latest_filing_subq.c.max_id
    ).order_by(
        desc(Filing13F.total_value)
    ).limit(limit).all()
    
    return [
        SuperinvestorListItem(
            cik=r.Superinvestor.cik,
            name=r.Superinvestor.name,
            firm=r.Superinvestor.firm,
            total_value=r.total_value,
            filing_date=str(r.filing_date) if r.filing_date else None,
            holdings_count=r.positions_count
        )
        for r in results
    ]


@app.get("/api/superinvestors/{cik}", response_model=SuperinvestorDetail)
async def get_superinvestor_detail(cik: str, db: Session = Depends(get_db)):
    """Get detailed holdings for a specific superinvestor"""
    
    investor = db.query(Superinvestor).filter_by(cik=cik).first()
    if not investor:
        raise HTTPException(status_code=404, detail="Superinvestor not found")
    
    # Get latest filing
    latest_filing = db.query(Filing13F).filter_by(
        superinvestor_id=investor.id
    ).order_by(desc(Filing13F.filing_date)).first()
    
    holdings = []
    if latest_filing:
        holdings_data = db.query(Holding).filter_by(
            filing_id=latest_filing.id
        ).order_by(desc(Holding.pct_portfolio)).all()
        
        holdings = [
            HoldingResponse(
                ticker=h.ticker,
                cusip=h.cusip,
                issuer_name=h.issuer_name,
                value=h.value or 0,
                shares=h.shares or 0,
                pct_portfolio=h.pct_portfolio,
                shares_change=h.shares_change,
                shares_change_pct=h.shares_change_pct,
                is_new=h.is_new or False,
                is_sold=h.is_sold or False
            )
            for h in holdings_data
        ]
    
    return SuperinvestorDetail(
        cik=investor.cik,
        name=investor.name,
        firm=investor.firm,
        filing_date=str(latest_filing.filing_date) if latest_filing else None,
        report_date=str(latest_filing.report_date) if latest_filing and latest_filing.report_date else None,
        total_value=latest_filing.total_value if latest_filing else None,
        holdings=holdings
    )


@app.get("/api/superinvestors/{cik}/history")
async def get_superinvestor_history(
    cik: str,
    limit: int = Query(8, ge=1, le=20),
    db: Session = Depends(get_db)
):
    """Get historical filings for a superinvestor"""
    
    investor = db.query(Superinvestor).filter_by(cik=cik).first()
    if not investor:
        raise HTTPException(status_code=404, detail="Superinvestor not found")
    
    filings = db.query(Filing13F).filter_by(
        superinvestor_id=investor.id
    ).order_by(desc(Filing13F.filing_date)).limit(limit).all()
    
    return [
        {
            "filing_date": str(f.filing_date),
            "report_date": str(f.report_date) if f.report_date else None,
            "total_value": f.total_value,
            "positions_count": f.positions_count
        }
        for f in filings
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# CONGRESS ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/congress/members", response_model=List[CongressMemberListItem])
async def get_congress_members(
    chamber: Optional[str] = Query(None, regex="^(House|Senate)$"),
    party: Optional[str] = Query(None, regex="^[DRI]$"),
    limit: int = Query(535, ge=1, le=600),
    db: Session = Depends(get_db)
):
    """Get all congress members with trade counts"""
    
    # Subquery for trade counts
    trade_counts = db.query(
        CongressTrade.member_id,
        func.count(CongressTrade.id).label('trade_count')
    ).group_by(CongressTrade.member_id).subquery()
    
    query = db.query(
        CongressMember,
        func.coalesce(trade_counts.c.trade_count, 0).label('trades')
    ).outerjoin(
        trade_counts,
        CongressMember.id == trade_counts.c.member_id
    )
    
    if chamber:
        query = query.filter(CongressMember.chamber == chamber)
    if party:
        query = query.filter(CongressMember.party == party)
    
    results = query.order_by(desc('trades')).limit(limit).all()
    
    return [
        CongressMemberListItem(
            bioguide_id=r.CongressMember.bioguide_id,
            name=r.CongressMember.name,
            party=r.CongressMember.party,
            chamber=r.CongressMember.chamber,
            state=r.CongressMember.state,
            trades_count=r.trades
        )
        for r in results
    ]


@app.get("/api/congress/members/{bioguide_id}")
async def get_congress_member_detail(bioguide_id: str, db: Session = Depends(get_db)):
    """Get detailed info for a congress member"""
    
    member = db.query(CongressMember).filter_by(bioguide_id=bioguide_id).first()
    if not member:
        raise HTTPException(status_code=404, detail="Congress member not found")
    
    trades_count = db.query(CongressTrade).filter_by(member_id=member.id).count()
    
    latest_networth = db.query(NetWorthReport).filter_by(
        member_id=member.id
    ).order_by(desc(NetWorthReport.report_year)).first()
    
    return {
        "bioguide_id": member.bioguide_id,
        "name": member.name,
        "party": member.party,
        "chamber": member.chamber,
        "state": member.state,
        "trades_count": trades_count,
        "net_worth": {
            "year": latest_networth.report_year if latest_networth else None,
            "min": latest_networth.net_worth_min if latest_networth else None,
            "max": latest_networth.net_worth_max if latest_networth else None,
        } if latest_networth else None
    }


@app.get("/api/congress/trades", response_model=List[CongressTradeResponse])
async def get_congress_trades(
    limit: int = Query(100, ge=1, le=500),
    party: Optional[str] = Query(None, regex="^[DRI]$"),
    chamber: Optional[str] = Query(None, regex="^(House|Senate)$"),
    ticker: Optional[str] = None,
    transaction_type: Optional[str] = Query(None, regex="^(Purchase|Sale)$"),
    days: int = Query(90, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """Get recent congressional trades with filters"""
    
    cutoff = datetime.now().date() - timedelta(days=days)
    
    query = db.query(CongressTrade, CongressMember).join(
        CongressMember, CongressTrade.member_id == CongressMember.id
    ).filter(CongressTrade.transaction_date >= cutoff)
    
    if party:
        query = query.filter(CongressMember.party == party)
    if chamber:
        query = query.filter(CongressMember.chamber == chamber)
    if ticker:
        query = query.filter(CongressTrade.ticker == ticker.upper())
    if transaction_type:
        query = query.filter(CongressTrade.transaction_type == transaction_type)
    
    results = query.order_by(desc(CongressTrade.transaction_date)).limit(limit).all()
    
    return [
        CongressTradeResponse(
            id=r.CongressTrade.id,
            member_name=r.CongressMember.name,
            party=r.CongressMember.party,
            chamber=r.CongressMember.chamber,
            state=r.CongressMember.state,
            ticker=r.CongressTrade.ticker,
            asset_name=r.CongressTrade.asset_name,
            transaction_type=r.CongressTrade.transaction_type,
            amount_range=r.CongressTrade.amount_range_text,
            transaction_date=str(r.CongressTrade.transaction_date) if r.CongressTrade.transaction_date else None,
            disclosure_date=str(r.CongressTrade.disclosure_date) if r.CongressTrade.disclosure_date else None
        )
        for r in results
    ]


@app.get("/api/congress/members/{bioguide_id}/trades", response_model=List[CongressTradeResponse])
async def get_member_trades(
    bioguide_id: str,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get trades for a specific congress member"""
    
    member = db.query(CongressMember).filter_by(bioguide_id=bioguide_id).first()
    if not member:
        raise HTTPException(status_code=404, detail="Congress member not found")
    
    trades = db.query(CongressTrade).filter_by(
        member_id=member.id
    ).order_by(desc(CongressTrade.transaction_date)).limit(limit).all()
    
    return [
        CongressTradeResponse(
            id=t.id,
            member_name=member.name,
            party=member.party,
            chamber=member.chamber,
            state=member.state,
            ticker=t.ticker,
            asset_name=t.asset_name,
            transaction_type=t.transaction_type,
            amount_range=t.amount_range_text,
            transaction_date=str(t.transaction_date) if t.transaction_date else None,
            disclosure_date=str(t.disclosure_date) if t.disclosure_date else None
        )
        for t in trades
    ]


@app.get("/api/congress/members/{bioguide_id}/networth", response_model=Optional[NetWorthResponse])
async def get_member_networth(bioguide_id: str, db: Session = Depends(get_db)):
    """Get net worth for a congress member"""
    
    member = db.query(CongressMember).filter_by(bioguide_id=bioguide_id).first()
    if not member:
        raise HTTPException(status_code=404, detail="Congress member not found")
    
    report = db.query(NetWorthReport).filter_by(
        member_id=member.id
    ).order_by(desc(NetWorthReport.report_year)).first()
    
    if not report:
        return None
    
    return NetWorthResponse(
        member_name=member.name,
        party=member.party,
        chamber=member.chamber,
        state=member.state,
        report_year=report.report_year,
        net_worth_min=report.net_worth_min,
        net_worth_max=report.net_worth_max,
        total_assets_min=report.total_assets_min,
        total_assets_max=report.total_assets_max,
        total_liabilities_min=report.total_liabilities_min,
        total_liabilities_max=report.total_liabilities_max,
        spouse_name=report.spouse_name
    )


# ═══════════════════════════════════════════════════════════════════════════════
# AGGREGATION / INSIGHTS ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/insights/aggregated", response_model=InsightsResponse)
async def get_aggregated_insights(db: Session = Depends(get_db)):
    """Get aggregated insights across all investors and congress"""
    
    # Get latest filing IDs for each superinvestor
    latest_filings = db.query(
        Filing13F.superinvestor_id,
        func.max(Filing13F.id).label('max_id')
    ).group_by(Filing13F.superinvestor_id).subquery()
    
    # Top superinvestor holdings (most commonly held stocks)
    top_holdings = db.query(
        Holding.ticker,
        Holding.issuer_name,
        func.count(Holding.superinvestor_id).label('holder_count'),
        func.sum(Holding.value).label('total_value')
    ).join(
        latest_filings,
        Holding.filing_id == latest_filings.c.max_id
    ).filter(
        Holding.ticker.isnot(None),
        Holding.is_sold == False
    ).group_by(
        Holding.ticker, Holding.issuer_name
    ).order_by(
        desc('holder_count')
    ).limit(5).all()
    
    # Top superinvestor buys (new positions)
    top_buys = db.query(
        Holding.ticker,
        Holding.issuer_name,
        func.count(Holding.superinvestor_id).label('buyer_count'),
        func.sum(Holding.value).label('total_value')
    ).join(
        latest_filings,
        Holding.filing_id == latest_filings.c.max_id
    ).filter(
        Holding.ticker.isnot(None),
        Holding.is_new == True
    ).group_by(
        Holding.ticker, Holding.issuer_name
    ).order_by(
        desc('buyer_count')
    ).limit(5).all()
    
    # Top superinvestor sells
    top_sells = db.query(
        Holding.ticker,
        Holding.issuer_name,
        func.count(Holding.superinvestor_id).label('seller_count')
    ).join(
        latest_filings,
        Holding.filing_id == latest_filings.c.max_id
    ).filter(
        Holding.ticker.isnot(None),
        Holding.is_sold == True
    ).group_by(
        Holding.ticker, Holding.issuer_name
    ).order_by(
        desc('seller_count')
    ).limit(5).all()
    
    # Congress trades aggregation (last 90 days)
    cutoff = datetime.now().date() - timedelta(days=90)
    
    # Top congress buys
    congress_buys = db.query(
        CongressTrade.ticker,
        CongressTrade.asset_name,
        func.count(CongressTrade.id).label('trade_count')
    ).filter(
        CongressTrade.ticker.isnot(None),
        CongressTrade.transaction_type == 'Purchase',
        CongressTrade.transaction_date >= cutoff
    ).group_by(
        CongressTrade.ticker, CongressTrade.asset_name
    ).order_by(
        desc('trade_count')
    ).limit(5).all()
    
    # Top congress sells
    congress_sells = db.query(
        CongressTrade.ticker,
        CongressTrade.asset_name,
        func.count(CongressTrade.id).label('trade_count')
    ).filter(
        CongressTrade.ticker.isnot(None),
        CongressTrade.transaction_type == 'Sale',
        CongressTrade.transaction_date >= cutoff
    ).group_by(
        CongressTrade.ticker, CongressTrade.asset_name
    ).order_by(
        desc('trade_count')
    ).limit(5).all()
    
    return InsightsResponse(
        top_superinvestor_holdings=[
            {"ticker": h.ticker, "name": h.issuer_name, "holders": h.holder_count, "value": h.total_value}
            for h in top_holdings
        ],
        top_superinvestor_buys=[
            {"ticker": b.ticker, "name": b.issuer_name, "buyers": b.buyer_count, "value": b.total_value}
            for b in top_buys
        ],
        top_superinvestor_sells=[
            {"ticker": s.ticker, "name": s.issuer_name, "sellers": s.seller_count}
            for s in top_sells
        ],
        top_congress_holdings=[],  # Would need a holdings table for congress
        top_congress_buys=[
            {"ticker": b.ticker, "name": b.asset_name, "trades": b.trade_count}
            for b in congress_buys
        ],
        top_congress_sells=[
            {"ticker": s.ticker, "name": s.asset_name, "trades": s.trade_count}
            for s in congress_sells
        ]
    )


@app.get("/api/insights/stock/{ticker}", response_model=StockHoldersResponse)
async def get_stock_holders(ticker: str, db: Session = Depends(get_db)):
    """Get all holders of a specific stock"""
    
    ticker = ticker.upper()
    
    # Get latest filing IDs
    latest_filings = db.query(
        Filing13F.superinvestor_id,
        func.max(Filing13F.id).label('max_id')
    ).group_by(Filing13F.superinvestor_id).subquery()
    
    # Superinvestors holding this stock
    superinvestor_holders = db.query(
        Superinvestor.name,
        Superinvestor.firm,
        Holding.value,
        Holding.shares,
        Holding.pct_portfolio,
        Holding.is_new
    ).join(
        Holding, Superinvestor.id == Holding.superinvestor_id
    ).join(
        latest_filings,
        Holding.filing_id == latest_filings.c.max_id
    ).filter(
        Holding.ticker == ticker,
        Holding.is_sold == False
    ).order_by(desc(Holding.value)).all()
    
    # Congress members who traded this stock recently
    cutoff = datetime.now().date() - timedelta(days=180)
    congress_trades = db.query(CongressTrade, CongressMember).join(
        CongressMember, CongressTrade.member_id == CongressMember.id
    ).filter(
        CongressTrade.ticker == ticker,
        CongressTrade.transaction_date >= cutoff
    ).order_by(desc(CongressTrade.transaction_date)).limit(20).all()
    
    return StockHoldersResponse(
        ticker=ticker,
        superinvestor_holders=[
            {
                "name": h.name,
                "firm": h.firm,
                "value": h.value,
                "shares": h.shares,
                "pct_portfolio": h.pct_portfolio,
                "is_new": h.is_new
            }
            for h in superinvestor_holders
        ],
        congress_holders=[],  # Would need congress holdings table
        recent_congress_trades=[
            CongressTradeResponse(
                id=t.CongressTrade.id,
                member_name=t.CongressMember.name,
                party=t.CongressMember.party,
                chamber=t.CongressMember.chamber,
                state=t.CongressMember.state,
                ticker=t.CongressTrade.ticker,
                asset_name=t.CongressTrade.asset_name,
                transaction_type=t.CongressTrade.transaction_type,
                amount_range=t.CongressTrade.amount_range_text,
                transaction_date=str(t.CongressTrade.transaction_date) if t.CongressTrade.transaction_date else None,
                disclosure_date=str(t.CongressTrade.disclosure_date) if t.CongressTrade.disclosure_date else None
            )
            for t in congress_trades
        ]
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
