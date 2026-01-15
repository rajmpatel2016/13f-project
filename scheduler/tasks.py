"""
Celery tasks for scheduled scraping
"""
import logging
from datetime import datetime, timedelta
from typing import List

from .celery_config import celery_app
from database import get_session, init_db
from database.services import (
    SuperinvestorService, FilingService, HoldingService,
    CongressService, TradeService, NetWorthService,
    ScraperJobService
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SEC 13F SCRAPING
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, max_retries=3)
def scrape_13f_filings(self):
    """
    Scrape latest 13F filings for all tracked superinvestors.
    Runs daily, but only processes new filings.
    """
    from scrapers.sec_13f_scraper import SEC13FScraper
    
    logger.info("Starting 13F filing scrape job")
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('sec_13f')
    
    try:
        scraper = SEC13FScraper()
        investor_service = SuperinvestorService(session)
        filing_service = FilingService(session)
        holding_service = HoldingService(session)
        
        records_processed = 0
        records_created = 0
        records_updated = 0
        
        # Get list of CIKs to track (from database or config)
        investors = session.query(Superinvestor).all()
        
        if not investors:
            # If no investors in DB, seed with default list
            investors = seed_default_superinvestors(session)
        
        for investor in investors:
            try:
                logger.info(f"Checking filings for {investor.name} (CIK: {investor.cik})")
                
                # Get latest filing from SEC
                filing_data = scraper.get_latest_filing(investor.cik)
                
                if not filing_data:
                    continue
                
                records_processed += 1
                
                # Check if we already have this filing
                existing = session.query(Filing13F).filter_by(
                    accession_number=filing_data['accession_number']
                ).first()
                
                if existing:
                    logger.info(f"  Filing {filing_data['accession_number']} already exists")
                    continue
                
                # Get previous filing for change calculation
                prev_filing = filing_service.get_latest_filing(investor.id)
                
                # Create new filing
                filing, created = filing_service.create_filing(
                    superinvestor_id=investor.id,
                    accession_number=filing_data['accession_number'],
                    filing_date=filing_data['filing_date'],
                    report_date=filing_data.get('report_date'),
                    total_value=filing_data.get('total_value'),
                    positions_count=len(filing_data.get('holdings', []))
                )
                
                if created:
                    records_created += 1
                    
                    # Add holdings with change tracking
                    holdings_count = holding_service.add_holdings_with_changes(
                        superinvestor_id=investor.id,
                        filing_id=filing.id,
                        holdings_data=filing_data.get('holdings', []),
                        previous_filing_id=prev_filing.id if prev_filing else None
                    )
                    
                    logger.info(f"  Created filing with {holdings_count} holdings")
                
                session.commit()
                
            except Exception as e:
                logger.error(f"Error processing {investor.name}: {e}")
                session.rollback()
                continue
        
        job_service.complete_job(job, records_processed, records_created, records_updated)
        logger.info(f"13F scrape completed: {records_processed} processed, {records_created} created")
        
        return {
            'status': 'success',
            'records_processed': records_processed,
            'records_created': records_created
        }
        
    except Exception as e:
        logger.error(f"13F scrape job failed: {e}")
        job_service.fail_job(job, str(e))
        self.retry(exc=e, countdown=300)  # Retry in 5 minutes
    
    finally:
        session.close()


@celery_app.task(bind=True, max_retries=3)
def scrape_single_investor(self, cik: str):
    """Scrape a single investor's latest filing"""
    from scrapers.sec_13f_scraper import SEC13FScraper
    
    logger.info(f"Scraping single investor: {cik}")
    
    session = get_session()
    
    try:
        scraper = SEC13FScraper()
        investor_service = SuperinvestorService(session)
        filing_service = FilingService(session)
        holding_service = HoldingService(session)
        
        # Get or create investor
        investor = investor_service.get_by_cik(cik)
        if not investor:
            # Fetch investor info and create
            info = scraper.get_filer_info(cik)
            investor, _ = investor_service.get_or_create(
                cik=cik,
                name=info.get('name', f'Unknown ({cik})'),
                firm=info.get('firm')
            )
        
        # Get latest filing
        filing_data = scraper.get_latest_filing(cik)
        
        if not filing_data:
            return {'status': 'no_filing_found'}
        
        # Get previous filing for change calculation
        prev_filing = filing_service.get_latest_filing(investor.id)
        
        # Create filing
        filing, created = filing_service.create_filing(
            superinvestor_id=investor.id,
            accession_number=filing_data['accession_number'],
            filing_date=filing_data['filing_date'],
            report_date=filing_data.get('report_date'),
            total_value=filing_data.get('total_value'),
            positions_count=len(filing_data.get('holdings', []))
        )
        
        if created:
            holding_service.add_holdings_with_changes(
                superinvestor_id=investor.id,
                filing_id=filing.id,
                holdings_data=filing_data.get('holdings', []),
                previous_filing_id=prev_filing.id if prev_filing else None
            )
        
        session.commit()
        
        return {
            'status': 'success',
            'created': created,
            'filing_date': str(filing_data['filing_date'])
        }
        
    except Exception as e:
        logger.error(f"Error scraping investor {cik}: {e}")
        session.rollback()
        raise
    
    finally:
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
# CONGRESS TRADES SCRAPING
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, max_retries=3)
def scrape_congress_trades(self):
    """
    Scrape latest congress stock trades from House/Senate disclosures.
    Runs every 6 hours to catch new filings quickly.
    """
    from scrapers.congress_disclosure_scraper import CongressDisclosureScraper
    
    logger.info("Starting Congress trades scrape job")
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('congress_trades')
    
    try:
        scraper = CongressDisclosureScraper()
        congress_service = CongressService(session)
        trade_service = TradeService(session)
        
        records_processed = 0
        records_created = 0
        
        # Get recent disclosures (last 7 days to catch any we missed)
        disclosures = scraper.get_recent_disclosures(days=7)
        
        for disclosure in disclosures:
            try:
                records_processed += 1
                
                # Get or create member
                member, _ = congress_service.get_or_create(
                    bioguide_id=disclosure.get('bioguide_id', f"unknown_{disclosure.get('name', 'X')}"),
                    name=disclosure.get('name'),
                    party=disclosure.get('party'),
                    chamber=disclosure.get('chamber'),
                    state=disclosure.get('state')
                )
                
                # Get trades from this disclosure
                trades = scraper.get_trades_from_disclosure(disclosure)
                
                for trade_data in trades:
                    trade, created = trade_service.create_trade(member.id, trade_data)
                    if created:
                        records_created += 1
                
                session.commit()
                
            except Exception as e:
                logger.error(f"Error processing disclosure: {e}")
                session.rollback()
                continue
        
        job_service.complete_job(job, records_processed, records_created, 0)
        logger.info(f"Congress trades scrape completed: {records_processed} processed, {records_created} created")
        
        return {
            'status': 'success',
            'records_processed': records_processed,
            'records_created': records_created
        }
        
    except Exception as e:
        logger.error(f"Congress trades scrape job failed: {e}")
        job_service.fail_job(job, str(e))
        self.retry(exc=e, countdown=300)
    
    finally:
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
# NET WORTH SCRAPING
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, max_retries=3)
def scrape_net_worth(self):
    """
    Scrape Annual Financial Disclosures for net worth data.
    Runs weekly since these are annual reports.
    """
    from scrapers.congress_disclosure_scraper import CongressDisclosureScraper
    
    logger.info("Starting net worth scrape job")
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('net_worth')
    
    try:
        scraper = CongressDisclosureScraper()
        congress_service = CongressService(session)
        net_worth_service = NetWorthService(session)
        
        records_processed = 0
        records_created = 0
        records_updated = 0
        
        # Get all active congress members
        members = congress_service.get_all()
        
        for member in members:
            try:
                records_processed += 1
                
                # Get latest AFD
                afd_data = scraper.get_annual_financial_disclosure(member.bioguide_id)
                
                if not afd_data:
                    continue
                
                # Create or update report
                report, created = net_worth_service.create_report(
                    member_id=member.id,
                    report_data={
                        'report_year': afd_data.get('report_year'),
                        'filing_date': afd_data.get('filing_date'),
                        'total_assets_min': afd_data.get('total_assets_min'),
                        'total_assets_max': afd_data.get('total_assets_max'),
                        'total_liabilities_min': afd_data.get('total_liabilities_min'),
                        'total_liabilities_max': afd_data.get('total_liabilities_max'),
                        'net_worth_min': afd_data.get('net_worth_min'),
                        'net_worth_max': afd_data.get('net_worth_max'),
                        'spouse_name': afd_data.get('spouse_name'),
                        'filing_url': afd_data.get('filing_url')
                    }
                )
                
                if created:
                    records_created += 1
                    
                    # Add assets
                    for asset in afd_data.get('assets', []):
                        net_worth_service.add_asset(report.id, asset)
                    
                    # Add liabilities
                    for liability in afd_data.get('liabilities', []):
                        net_worth_service.add_liability(report.id, liability)
                else:
                    records_updated += 1
                
                session.commit()
                
            except Exception as e:
                logger.error(f"Error processing net worth for {member.name}: {e}")
                session.rollback()
                continue
        
        job_service.complete_job(job, records_processed, records_created, records_updated)
        logger.info(f"Net worth scrape completed: {records_processed} processed, {records_created} created, {records_updated} updated")
        
        return {
            'status': 'success',
            'records_processed': records_processed,
            'records_created': records_created,
            'records_updated': records_updated
        }
        
    except Exception as e:
        logger.error(f"Net worth scrape job failed: {e}")
        job_service.fail_job(job, str(e))
        self.retry(exc=e, countdown=300)
    
    finally:
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY TASKS
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task
def enrich_stock_data():
    """Enrich stock reference data (ticker, name, sector, etc.)"""
    logger.info("Starting stock data enrichment")
    # TODO: Integrate with a stock API (Yahoo Finance, Alpha Vantage, etc.)
    return {'status': 'success'}


@celery_app.task
def cleanup_old_jobs():
    """Clean up old scraper job records (keep last 30 days)"""
    logger.info("Starting job cleanup")
    
    session = get_session()
    
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        deleted = session.query(ScraperJob).filter(
            ScraperJob.created_at < cutoff
        ).delete()
        
        session.commit()
        logger.info(f"Deleted {deleted} old job records")
        
        return {'status': 'success', 'deleted': deleted}
        
    finally:
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SEEDING
# ═══════════════════════════════════════════════════════════════════════════════

def seed_default_superinvestors(session) -> List:
    """Seed the database with default superinvestors to track"""
    from database.services import SuperinvestorService
    
    service = SuperinvestorService(session)
    
    # Top superinvestors from Dataroma
    default_investors = [
        ("0001067983", "Warren Buffett", "Berkshire Hathaway"),
        ("0001336528", "Bill Ackman", "Pershing Square Capital"),
        ("0001649339", "Michael Burry", "Scion Asset Management"),
        ("0001061768", "Seth Klarman", "Baupost Group"),
        ("0001656456", "David Tepper", "Appaloosa Management"),
        ("0000921669", "David Einhorn", "Greenlight Capital"),
        ("0001167483", "Bill & Melinda Gates Foundation", "Gates Foundation Trust"),
        ("0001040273", "Chase Coleman", "Tiger Global Management"),
        ("0000905191", "Chris Hohn", "TCI Fund Management"),
        ("0001541617", "Nelson Peltz", "Trian Fund Management"),
        ("0000885508", "Carl Icahn", "Icahn Capital Management"),
        ("0001079114", "Daniel Loeb", "Third Point"),
        ("0001029160", "Leon Cooperman", "Omega Advisors"),
        ("0000860643", "Howard Marks", "Oaktree Capital"),
        ("0000895421", "Mohnish Pabrai", "Pabrai Investment Funds"),
        ("0001135730", "Guy Spier", "Aquamarine Capital"),
        ("0001766389", "Terry Smith", "Fundsmith"),
        ("0001062047", "Thomas Russo", "Gardner Russo & Quinn"),
        ("0001099281", "Li Lu", "Himalaya Capital"),
        ("0000783412", "Chuck Akre", "Akre Capital Management"),
    ]
    
    investors = []
    for cik, name, firm in default_investors:
        investor, created = service.get_or_create(cik, name, firm)
        investors.append(investor)
        if created:
            logger.info(f"Seeded superinvestor: {name}")
    
    session.commit()
    return investors
