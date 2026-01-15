"""
Database service layer for InvestorInsight
Handles all database operations with proper historical tracking.
"""
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from sqlalchemy import func, desc, and_
from sqlalchemy.orm import Session

from .models import (
    Superinvestor, Filing13F, Holding,
    CongressMember, CongressTrade, NetWorthReport, NetWorthAsset, NetWorthLiability,
    Stock, ScraperJob
)


class SuperinvestorService:
    """Service for superinvestor operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create(self, cik: str, name: str, firm: str = None) -> Tuple[Superinvestor, bool]:
        """Get existing superinvestor or create new one"""
        investor = self.session.query(Superinvestor).filter_by(cik=cik).first()
        if investor:
            # Update if name/firm changed
            if name and investor.name != name:
                investor.name = name
            if firm and investor.firm != firm:
                investor.firm = firm
            return investor, False
        
        investor = Superinvestor(cik=cik, name=name, firm=firm)
        self.session.add(investor)
        self.session.flush()
        return investor, True
    
    def get_all(self, limit: int = 100) -> List[Superinvestor]:
        """Get all superinvestors sorted by latest portfolio value"""
        # Subquery to get latest filing for each superinvestor
        latest_filing = self.session.query(
            Filing13F.superinvestor_id,
            func.max(Filing13F.filing_date).label('max_date')
        ).group_by(Filing13F.superinvestor_id).subquery()
        
        return self.session.query(Superinvestor).join(
            Filing13F, Superinvestor.id == Filing13F.superinvestor_id
        ).join(
            latest_filing, and_(
                Filing13F.superinvestor_id == latest_filing.c.superinvestor_id,
                Filing13F.filing_date == latest_filing.c.max_date
            )
        ).order_by(desc(Filing13F.total_value)).limit(limit).all()
    
    def get_by_cik(self, cik: str) -> Optional[Superinvestor]:
        """Get superinvestor by CIK"""
        return self.session.query(Superinvestor).filter_by(cik=cik).first()


class FilingService:
    """Service for 13F filing operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create_filing(
        self, 
        superinvestor_id: int,
        accession_number: str,
        filing_date: date,
        report_date: date = None,
        total_value: int = None,
        positions_count: int = None
    ) -> Tuple[Filing13F, bool]:
        """Create a new filing if it doesn't exist"""
        existing = self.session.query(Filing13F).filter_by(
            accession_number=accession_number
        ).first()
        
        if existing:
            return existing, False
        
        filing = Filing13F(
            superinvestor_id=superinvestor_id,
            accession_number=accession_number,
            filing_date=filing_date,
            report_date=report_date,
            total_value=total_value,
            positions_count=positions_count
        )
        self.session.add(filing)
        self.session.flush()
        return filing, True
    
    def get_latest_filing(self, superinvestor_id: int) -> Optional[Filing13F]:
        """Get most recent filing for a superinvestor"""
        return self.session.query(Filing13F).filter_by(
            superinvestor_id=superinvestor_id
        ).order_by(desc(Filing13F.filing_date)).first()
    
    def get_previous_filing(self, superinvestor_id: int, before_date: date) -> Optional[Filing13F]:
        """Get the filing before a given date"""
        return self.session.query(Filing13F).filter(
            Filing13F.superinvestor_id == superinvestor_id,
            Filing13F.filing_date < before_date
        ).order_by(desc(Filing13F.filing_date)).first()


class HoldingService:
    """Service for holdings operations with change tracking"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def add_holdings_with_changes(
        self,
        superinvestor_id: int,
        filing_id: int,
        holdings_data: List[Dict],
        previous_filing_id: int = None
    ) -> int:
        """
        Add holdings and calculate changes from previous filing.
        Returns number of holdings added.
        """
        # Get previous holdings map for change calculation
        prev_holdings = {}
        if previous_filing_id:
            prev = self.session.query(Holding).filter_by(filing_id=previous_filing_id).all()
            prev_holdings = {h.ticker or h.cusip: h for h in prev}
        
        count = 0
        for h in holdings_data:
            ticker = h.get('ticker')
            cusip = h.get('cusip')
            shares = h.get('shares', 0)
            value = h.get('value', 0)
            
            # Calculate changes
            key = ticker or cusip
            prev = prev_holdings.get(key)
            
            shares_change = None
            shares_change_pct = None
            is_new = False
            
            if prev:
                shares_change = shares - (prev.shares or 0)
                if prev.shares and prev.shares > 0:
                    shares_change_pct = (shares_change / prev.shares) * 100
            else:
                is_new = True
            
            holding = Holding(
                superinvestor_id=superinvestor_id,
                filing_id=filing_id,
                cusip=cusip,
                ticker=ticker,
                issuer_name=h.get('issuer_name'),
                shares=shares,
                value=value,
                pct_portfolio=h.get('pct_portfolio'),
                shares_change=shares_change,
                shares_change_pct=shares_change_pct,
                is_new=is_new,
                is_sold=False
            )
            self.session.add(holding)
            count += 1
        
        # Mark sold positions (in prev but not in current)
        current_keys = {h.get('ticker') or h.get('cusip') for h in holdings_data}
        for key, prev_holding in prev_holdings.items():
            if key not in current_keys:
                # Create a "sold" record
                sold = Holding(
                    superinvestor_id=superinvestor_id,
                    filing_id=filing_id,
                    cusip=prev_holding.cusip,
                    ticker=prev_holding.ticker,
                    issuer_name=prev_holding.issuer_name,
                    shares=0,
                    value=0,
                    pct_portfolio=0,
                    shares_change=-prev_holding.shares if prev_holding.shares else None,
                    shares_change_pct=-100,
                    is_new=False,
                    is_sold=True
                )
                self.session.add(sold)
        
        self.session.flush()
        return count
    
    def get_holdings_for_filing(self, filing_id: int) -> List[Holding]:
        """Get all holdings for a filing"""
        return self.session.query(Holding).filter_by(
            filing_id=filing_id
        ).order_by(desc(Holding.pct_portfolio)).all()
    
    def get_top_holdings_by_ticker(self, ticker: str, limit: int = 20) -> List[Dict]:
        """Get superinvestors holding a specific ticker"""
        # Get latest filing for each superinvestor
        latest_filing = self.session.query(
            Filing13F.superinvestor_id,
            func.max(Filing13F.id).label('max_id')
        ).group_by(Filing13F.superinvestor_id).subquery()
        
        holdings = self.session.query(Holding, Superinvestor).join(
            Superinvestor, Holding.superinvestor_id == Superinvestor.id
        ).join(
            latest_filing, and_(
                Holding.filing_id == latest_filing.c.max_id,
                Holding.superinvestor_id == latest_filing.c.superinvestor_id
            )
        ).filter(
            Holding.ticker == ticker,
            Holding.is_sold == False
        ).order_by(desc(Holding.value)).limit(limit).all()
        
        return holdings


class CongressService:
    """Service for congress member operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create(
        self, 
        bioguide_id: str, 
        name: str, 
        party: str = None,
        chamber: str = None,
        state: str = None
    ) -> Tuple[CongressMember, bool]:
        """Get existing congress member or create new one"""
        member = self.session.query(CongressMember).filter_by(bioguide_id=bioguide_id).first()
        if member:
            # Update fields if changed
            if party and member.party != party:
                member.party = party
            if chamber and member.chamber != chamber:
                member.chamber = chamber
            if state and member.state != state:
                member.state = state
            return member, False
        
        member = CongressMember(
            bioguide_id=bioguide_id,
            name=name,
            party=party,
            chamber=chamber,
            state=state
        )
        self.session.add(member)
        self.session.flush()
        return member, True
    
    def get_all(self, chamber: str = None, party: str = None, limit: int = 535) -> List[CongressMember]:
        """Get congress members with optional filters"""
        query = self.session.query(CongressMember).filter_by(is_active=True)
        
        if chamber:
            query = query.filter_by(chamber=chamber)
        if party:
            query = query.filter_by(party=party)
        
        # Order by trade count (subquery)
        trade_count = self.session.query(
            CongressTrade.member_id,
            func.count(CongressTrade.id).label('trade_count')
        ).group_by(CongressTrade.member_id).subquery()
        
        query = query.outerjoin(
            trade_count, CongressMember.id == trade_count.c.member_id
        ).order_by(desc(func.coalesce(trade_count.c.trade_count, 0)))
        
        return query.limit(limit).all()
    
    def get_by_bioguide_id(self, bioguide_id: str) -> Optional[CongressMember]:
        """Get congress member by bioguide ID"""
        return self.session.query(CongressMember).filter_by(bioguide_id=bioguide_id).first()


class TradeService:
    """Service for congress trade operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create_trade(self, member_id: int, trade_data: Dict) -> Tuple[CongressTrade, bool]:
        """Create a trade if it doesn't exist (by ptr_id)"""
        ptr_id = trade_data.get('ptr_id')
        if ptr_id:
            existing = self.session.query(CongressTrade).filter_by(ptr_id=ptr_id).first()
            if existing:
                return existing, False
        
        trade = CongressTrade(
            member_id=member_id,
            transaction_date=trade_data.get('transaction_date'),
            disclosure_date=trade_data.get('disclosure_date'),
            ticker=trade_data.get('ticker'),
            asset_name=trade_data.get('asset_name'),
            asset_type=trade_data.get('asset_type'),
            transaction_type=trade_data.get('transaction_type'),
            amount_range_min=trade_data.get('amount_range_min'),
            amount_range_max=trade_data.get('amount_range_max'),
            amount_range_text=trade_data.get('amount_range_text'),
            owner=trade_data.get('owner'),
            ptr_id=ptr_id,
            filing_url=trade_data.get('filing_url')
        )
        self.session.add(trade)
        self.session.flush()
        return trade, True
    
    def get_recent_trades(self, days: int = 30, limit: int = 100) -> List[CongressTrade]:
        """Get recent trades across all members"""
        cutoff = datetime.utcnow().date()
        return self.session.query(CongressTrade).filter(
            CongressTrade.transaction_date >= cutoff
        ).order_by(desc(CongressTrade.transaction_date)).limit(limit).all()
    
    def get_trades_for_member(self, member_id: int, limit: int = 100) -> List[CongressTrade]:
        """Get trades for a specific member"""
        return self.session.query(CongressTrade).filter_by(
            member_id=member_id
        ).order_by(desc(CongressTrade.transaction_date)).limit(limit).all()
    
    def get_trades_by_ticker(self, ticker: str, limit: int = 50) -> List[Tuple[CongressTrade, CongressMember]]:
        """Get all trades for a ticker"""
        return self.session.query(CongressTrade, CongressMember).join(
            CongressMember, CongressTrade.member_id == CongressMember.id
        ).filter(
            CongressTrade.ticker == ticker
        ).order_by(desc(CongressTrade.transaction_date)).limit(limit).all()


class NetWorthService:
    """Service for net worth report operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create_report(self, member_id: int, report_data: Dict) -> Tuple[NetWorthReport, bool]:
        """Create or update net worth report for a year"""
        year = report_data.get('report_year')
        
        existing = self.session.query(NetWorthReport).filter_by(
            member_id=member_id, report_year=year
        ).first()
        
        if existing:
            # Update existing
            for key, value in report_data.items():
                if hasattr(existing, key) and value is not None:
                    setattr(existing, key, value)
            return existing, False
        
        report = NetWorthReport(
            member_id=member_id,
            **report_data
        )
        self.session.add(report)
        self.session.flush()
        return report, True
    
    def add_asset(self, report_id: int, asset_data: Dict) -> NetWorthAsset:
        """Add an asset to a net worth report"""
        asset = NetWorthAsset(report_id=report_id, **asset_data)
        self.session.add(asset)
        return asset
    
    def add_liability(self, report_id: int, liability_data: Dict) -> NetWorthLiability:
        """Add a liability to a net worth report"""
        liability = NetWorthLiability(report_id=report_id, **liability_data)
        self.session.add(liability)
        return liability
    
    def get_latest_report(self, member_id: int) -> Optional[NetWorthReport]:
        """Get most recent net worth report for a member"""
        return self.session.query(NetWorthReport).filter_by(
            member_id=member_id
        ).order_by(desc(NetWorthReport.report_year)).first()
    
    def get_net_worth_history(self, member_id: int) -> List[NetWorthReport]:
        """Get all net worth reports for a member (for historical chart)"""
        return self.session.query(NetWorthReport).filter_by(
            member_id=member_id
        ).order_by(NetWorthReport.report_year).all()


class ScraperJobService:
    """Service for tracking scraper jobs"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def start_job(self, job_type: str) -> ScraperJob:
        """Start a new scraper job"""
        job = ScraperJob(
            job_type=job_type,
            status='running',
            started_at=datetime.utcnow()
        )
        self.session.add(job)
        self.session.commit()
        return job
    
    def complete_job(self, job: ScraperJob, records_processed: int, records_created: int, records_updated: int):
        """Mark job as completed"""
        job.status = 'completed'
        job.completed_at = datetime.utcnow()
        job.records_processed = records_processed
        job.records_created = records_created
        job.records_updated = records_updated
        self.session.commit()
    
    def fail_job(self, job: ScraperJob, error_message: str):
        """Mark job as failed"""
        job.status = 'failed'
        job.completed_at = datetime.utcnow()
        job.error_message = error_message
        self.session.commit()
    
    def get_last_successful_job(self, job_type: str) -> Optional[ScraperJob]:
        """Get the last successful job of a type"""
        return self.session.query(ScraperJob).filter_by(
            job_type=job_type, status='completed'
        ).order_by(desc(ScraperJob.completed_at)).first()
