"""
Database models for InvestorInsight
Tracks superinvestors, congress members, holdings, trades, and net worth over time.
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, Date, 
    ForeignKey, Boolean, Text, BigInteger, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


# ═══════════════════════════════════════════════════════════════════════════════
# SUPERINVESTOR MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class Superinvestor(Base):
    """Superinvestor/fund manager profile"""
    __tablename__ = 'superinvestors'
    
    id = Column(Integer, primary_key=True)
    cik = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    firm = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    filings = relationship("Filing13F", back_populates="superinvestor", order_by="desc(Filing13F.filing_date)")
    holdings = relationship("Holding", back_populates="superinvestor")
    
    def __repr__(self):
        return f"<Superinvestor(cik={self.cik}, name={self.name})>"


class Filing13F(Base):
    """SEC 13F Filing - one per quarter per superinvestor"""
    __tablename__ = 'filings_13f'
    
    id = Column(Integer, primary_key=True)
    superinvestor_id = Column(Integer, ForeignKey('superinvestors.id'), nullable=False)
    accession_number = Column(String(50), unique=True, nullable=False)
    filing_date = Column(Date, nullable=False)
    report_date = Column(Date)  # Quarter end date
    total_value = Column(BigInteger)  # Total portfolio value in dollars
    positions_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    superinvestor = relationship("Superinvestor", back_populates="filings")
    holdings = relationship("Holding", back_populates="filing")
    
    __table_args__ = (
        Index('idx_filing_date', 'superinvestor_id', 'filing_date'),
    )


class Holding(Base):
    """Individual stock holding from a 13F filing"""
    __tablename__ = 'holdings'
    
    id = Column(Integer, primary_key=True)
    superinvestor_id = Column(Integer, ForeignKey('superinvestors.id'), nullable=False)
    filing_id = Column(Integer, ForeignKey('filings_13f.id'), nullable=False)
    
    # Stock info
    cusip = Column(String(20), index=True)
    ticker = Column(String(20), index=True)
    issuer_name = Column(String(255))
    
    # Position data
    shares = Column(BigInteger)
    value = Column(BigInteger)  # Value in dollars
    pct_portfolio = Column(Float)  # Percentage of total portfolio
    
    # Change tracking (compared to previous filing)
    shares_change = Column(BigInteger)  # Absolute change
    shares_change_pct = Column(Float)  # Percentage change
    is_new = Column(Boolean, default=False)  # New position this quarter
    is_sold = Column(Boolean, default=False)  # Position was sold (value = 0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    superinvestor = relationship("Superinvestor", back_populates="holdings")
    filing = relationship("Filing13F", back_populates="holdings")
    
    __table_args__ = (
        Index('idx_holding_ticker', 'ticker', 'filing_id'),
        Index('idx_holding_superinvestor', 'superinvestor_id', 'filing_id'),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CONGRESS MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class CongressMember(Base):
    """Congress member profile"""
    __tablename__ = 'congress_members'
    
    id = Column(Integer, primary_key=True)
    bioguide_id = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    party = Column(String(1))  # D, R, I
    chamber = Column(String(10))  # House, Senate
    state = Column(String(2))
    district = Column(String(10))  # For House members
    
    # Status
    is_active = Column(Boolean, default=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    trades = relationship("CongressTrade", back_populates="member", order_by="desc(CongressTrade.transaction_date)")
    net_worth_reports = relationship("NetWorthReport", back_populates="member", order_by="desc(NetWorthReport.report_year)")
    
    def __repr__(self):
        return f"<CongressMember(bioguide_id={self.bioguide_id}, name={self.name})>"


class CongressTrade(Base):
    """Individual stock trade by congress member (STOCK Act disclosure)"""
    __tablename__ = 'congress_trades'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('congress_members.id'), nullable=False)
    
    # Trade details
    transaction_date = Column(Date, nullable=False, index=True)
    disclosure_date = Column(Date)  # When it was publicly disclosed
    
    # Stock info
    ticker = Column(String(20), index=True)
    asset_name = Column(String(500))
    asset_type = Column(String(100))  # Stock, Stock Option, Bond, etc.
    
    # Transaction info
    transaction_type = Column(String(50))  # Purchase, Sale, Exchange
    amount_range_min = Column(BigInteger)  # Min of reported range
    amount_range_max = Column(BigInteger)  # Max of reported range
    amount_range_text = Column(String(50))  # Original text like "$1,001 - $15,000"
    
    # Owner
    owner = Column(String(20))  # Self, Spouse, Joint, Child
    
    # Filing info
    ptr_id = Column(String(50), unique=True)  # Unique ID from disclosure
    filing_url = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    member = relationship("CongressMember", back_populates="trades")
    
    __table_args__ = (
        Index('idx_trade_date_ticker', 'transaction_date', 'ticker'),
        Index('idx_trade_member', 'member_id', 'transaction_date'),
    )


class NetWorthReport(Base):
    """Annual Financial Disclosure - net worth snapshot"""
    __tablename__ = 'net_worth_reports'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('congress_members.id'), nullable=False)
    
    report_year = Column(Integer, nullable=False)
    filing_date = Column(Date)
    
    # Totals (ranges)
    total_assets_min = Column(BigInteger)
    total_assets_max = Column(BigInteger)
    total_liabilities_min = Column(BigInteger)
    total_liabilities_max = Column(BigInteger)
    net_worth_min = Column(BigInteger)
    net_worth_max = Column(BigInteger)
    
    # Earned income
    earned_income_min = Column(BigInteger)
    earned_income_max = Column(BigInteger)
    
    # Spouse info
    spouse_name = Column(String(255))
    
    filing_url = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    member = relationship("CongressMember", back_populates="net_worth_reports")
    assets = relationship("NetWorthAsset", back_populates="report")
    liabilities = relationship("NetWorthLiability", back_populates="report")
    
    __table_args__ = (
        UniqueConstraint('member_id', 'report_year', name='uq_member_year'),
    )


class NetWorthAsset(Base):
    """Individual asset from net worth report"""
    __tablename__ = 'net_worth_assets'
    
    id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey('net_worth_reports.id'), nullable=False)
    
    category = Column(String(100))  # Real Estate, Stocks, Retirement, etc.
    description = Column(Text)
    owner = Column(String(20))  # Self, Spouse, Joint
    
    value_min = Column(BigInteger)
    value_max = Column(BigInteger)
    
    # For stocks
    ticker = Column(String(20), index=True)
    
    # Income from this asset
    income_type = Column(String(100))
    income_min = Column(BigInteger)
    income_max = Column(BigInteger)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    report = relationship("NetWorthReport", back_populates="assets")


class NetWorthLiability(Base):
    """Individual liability from net worth report"""
    __tablename__ = 'net_worth_liabilities'
    
    id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey('net_worth_reports.id'), nullable=False)
    
    category = Column(String(100))  # Mortgage, Credit Card, Loan, etc.
    description = Column(Text)
    creditor = Column(String(255))
    owner = Column(String(20))
    
    value_min = Column(BigInteger)
    value_max = Column(BigInteger)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    report = relationship("NetWorthReport", back_populates="liabilities")


# ═══════════════════════════════════════════════════════════════════════════════
# STOCK / TICKER REFERENCE
# ═══════════════════════════════════════════════════════════════════════════════

class Stock(Base):
    """Stock/security reference data"""
    __tablename__ = 'stocks'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), unique=True, nullable=False, index=True)
    cusip = Column(String(20), index=True)
    name = Column(String(255))
    sector = Column(String(100))
    industry = Column(String(100))
    market_cap = Column(BigInteger)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPER JOB TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class ScraperJob(Base):
    """Track scraper runs for monitoring and deduplication"""
    __tablename__ = 'scraper_jobs'
    
    id = Column(Integer, primary_key=True)
    job_type = Column(String(50), nullable=False)  # sec_13f, congress_trades, net_worth
    status = Column(String(20), default='pending')  # pending, running, completed, failed
    
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    records_processed = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    
    error_message = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_job_type_status', 'job_type', 'status'),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine(database_url: str = None):
    """Create database engine"""
    if database_url is None:
        database_url = "sqlite:///./data/investorinsight.db"
    return create_engine(database_url, echo=False)


def get_session(engine=None):
    """Create database session"""
    if engine is None:
        engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db(database_url: str = None):
    """Initialize database - create all tables"""
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    return engine


if __name__ == "__main__":
    # Create tables
    engine = init_db()
    print("Database initialized successfully!")
    print(f"Tables created: {list(Base.metadata.tables.keys())}")
