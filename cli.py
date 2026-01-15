#!/usr/bin/env python3
"""
CLI tool for managing the InvestorInsight data pipeline.

Usage:
    python cli.py init-db                    # Initialize database
    python cli.py scrape 13f                 # Run 13F scraper
    python cli.py scrape congress            # Run Congress trades scraper
    python cli.py scrape networth            # Run net worth scraper
    python cli.py scrape all                 # Run all scrapers
    python cli.py add-investor <cik>         # Add a new superinvestor to track
    python cli.py status                     # Show scraper job status
    python cli.py stats                      # Show database statistics
"""
import argparse
import sys
from datetime import datetime, timedelta
from tabulate import tabulate

from database import init_db, get_session
from database.models import (
    Superinvestor, Filing13F, Holding,
    CongressMember, CongressTrade, NetWorthReport,
    ScraperJob
)
from database.services import (
    SuperinvestorService, FilingService, HoldingService,
    CongressService, TradeService, NetWorthService,
    ScraperJobService
)


def cmd_init_db(args):
    """Initialize the database"""
    print("Initializing database...")
    engine = init_db()
    print(f"Database initialized successfully!")
    print(f"Database URL: {engine.url}")


def cmd_scrape(args):
    """Run scrapers"""
    scrape_type = args.type
    
    if scrape_type == '13f' or scrape_type == 'all':
        print("\n" + "="*60)
        print("Running SEC 13F Scraper...")
        print("="*60)
        run_13f_scraper()
    
    if scrape_type == 'congress' or scrape_type == 'all':
        print("\n" + "="*60)
        print("Running Congress Trades Scraper...")
        print("="*60)
        run_congress_scraper()
    
    if scrape_type == 'networth' or scrape_type == 'all':
        print("\n" + "="*60)
        print("Running Net Worth Scraper...")
        print("="*60)
        run_networth_scraper()
    
    print("\nScraping complete!")


def run_13f_scraper():
    """Run the 13F scraper synchronously"""
    from scrapers.sec_13f_scraper import SEC13FScraper
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('sec_13f')
    
    try:
        scraper = SEC13FScraper()
        investor_service = SuperinvestorService(session)
        filing_service = FilingService(session)
        holding_service = HoldingService(session)
        
        # Get all tracked investors
        investors = session.query(Superinvestor).all()
        
        if not investors:
            print("No superinvestors in database. Seeding defaults...")
            from scheduler.tasks import seed_default_superinvestors
            investors = seed_default_superinvestors(session)
        
        records_processed = 0
        records_created = 0
        
        for investor in investors:
            print(f"  Checking {investor.name}...", end=" ")
            
            try:
                filing_data = scraper.get_latest_filing(investor.cik)
                records_processed += 1
                
                if not filing_data:
                    print("No filing found")
                    continue
                
                # Check if exists
                existing = session.query(Filing13F).filter_by(
                    accession_number=filing_data['accession_number']
                ).first()
                
                if existing:
                    print(f"Already have {filing_data['filing_date']}")
                    continue
                
                # Get previous for changes
                prev_filing = filing_service.get_latest_filing(investor.id)
                
                # Create filing
                filing, _ = filing_service.create_filing(
                    superinvestor_id=investor.id,
                    accession_number=filing_data['accession_number'],
                    filing_date=filing_data['filing_date'],
                    report_date=filing_data.get('report_date'),
                    total_value=filing_data.get('total_value'),
                    positions_count=len(filing_data.get('holdings', []))
                )
                
                # Add holdings
                holdings_count = holding_service.add_holdings_with_changes(
                    superinvestor_id=investor.id,
                    filing_id=filing.id,
                    holdings_data=filing_data.get('holdings', []),
                    previous_filing_id=prev_filing.id if prev_filing else None
                )
                
                records_created += 1
                print(f"NEW! {filing_data['filing_date']} ({holdings_count} holdings)")
                
                session.commit()
                
            except Exception as e:
                print(f"Error: {e}")
                session.rollback()
        
        job_service.complete_job(job, records_processed, records_created, 0)
        print(f"\n  Processed: {records_processed}, Created: {records_created}")
        
    except Exception as e:
        job_service.fail_job(job, str(e))
        print(f"Error: {e}")
        raise
    finally:
        session.close()


def run_congress_scraper():
    """Run the Congress trades scraper synchronously"""
    from scrapers.congress_disclosure_scraper import CongressDisclosureScraper
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('congress_trades')
    
    try:
        scraper = CongressDisclosureScraper()
        congress_service = CongressService(session)
        trade_service = TradeService(session)
        
        records_processed = 0
        records_created = 0
        
        print("  Fetching recent disclosures...")
        disclosures = scraper.get_recent_disclosures(days=30)
        print(f"  Found {len(disclosures)} disclosures")
        
        for disclosure in disclosures:
            try:
                records_processed += 1
                
                member, created = congress_service.get_or_create(
                    bioguide_id=disclosure.get('bioguide_id', f"unknown_{records_processed}"),
                    name=disclosure.get('name'),
                    party=disclosure.get('party'),
                    chamber=disclosure.get('chamber'),
                    state=disclosure.get('state')
                )
                
                if created:
                    print(f"  New member: {member.name}")
                
                trades = scraper.get_trades_from_disclosure(disclosure)
                
                for trade_data in trades:
                    trade, created = trade_service.create_trade(member.id, trade_data)
                    if created:
                        records_created += 1
                        print(f"    {member.name}: {trade_data.get('transaction_type')} {trade_data.get('ticker')}")
                
                session.commit()
                
            except Exception as e:
                print(f"  Error: {e}")
                session.rollback()
        
        job_service.complete_job(job, records_processed, records_created, 0)
        print(f"\n  Processed: {records_processed}, Created: {records_created}")
        
    except Exception as e:
        job_service.fail_job(job, str(e))
        print(f"Error: {e}")
        raise
    finally:
        session.close()


def run_networth_scraper():
    """Run the net worth scraper synchronously"""
    from scrapers.congress_disclosure_scraper import CongressDisclosureScraper
    
    session = get_session()
    job_service = ScraperJobService(session)
    job = job_service.start_job('net_worth')
    
    try:
        scraper = CongressDisclosureScraper()
        congress_service = CongressService(session)
        net_worth_service = NetWorthService(session)
        
        records_processed = 0
        records_created = 0
        
        members = session.query(CongressMember).limit(50).all()  # Limit for testing
        
        for member in members:
            try:
                print(f"  Checking {member.name}...", end=" ")
                records_processed += 1
                
                afd_data = scraper.get_annual_financial_disclosure(member.bioguide_id)
                
                if not afd_data:
                    print("No AFD found")
                    continue
                
                report, created = net_worth_service.create_report(
                    member_id=member.id,
                    report_data={
                        'report_year': afd_data.get('report_year'),
                        'total_assets_min': afd_data.get('total_assets_min'),
                        'total_assets_max': afd_data.get('total_assets_max'),
                        'net_worth_min': afd_data.get('net_worth_min'),
                        'net_worth_max': afd_data.get('net_worth_max'),
                    }
                )
                
                if created:
                    records_created += 1
                    print(f"NEW! {afd_data.get('report_year')}")
                else:
                    print("Already exists")
                
                session.commit()
                
            except Exception as e:
                print(f"Error: {e}")
                session.rollback()
        
        job_service.complete_job(job, records_processed, records_created, 0)
        print(f"\n  Processed: {records_processed}, Created: {records_created}")
        
    except Exception as e:
        job_service.fail_job(job, str(e))
        print(f"Error: {e}")
        raise
    finally:
        session.close()


def cmd_add_investor(args):
    """Add a new superinvestor to track"""
    cik = args.cik.zfill(10)  # Pad with zeros
    
    session = get_session()
    
    try:
        service = SuperinvestorService(session)
        
        # Check if already exists
        existing = service.get_by_cik(cik)
        if existing:
            print(f"Investor already tracked: {existing.name}")
            return
        
        # Create with placeholder name (will be updated on first scrape)
        investor, created = service.get_or_create(
            cik=cik,
            name=args.name or f"Unknown ({cik})",
            firm=args.firm
        )
        
        session.commit()
        print(f"Added investor: {investor.name} (CIK: {cik})")
        
        # Optionally trigger immediate scrape
        if args.scrape:
            print("Scraping latest filing...")
            from scheduler.tasks import scrape_single_investor
            result = scrape_single_investor(cik)
            print(f"Result: {result}")
        
    finally:
        session.close()


def cmd_status(args):
    """Show scraper job status"""
    session = get_session()
    
    try:
        jobs = session.query(ScraperJob).order_by(
            ScraperJob.created_at.desc()
        ).limit(20).all()
        
        if not jobs:
            print("No scraper jobs found")
            return
        
        table = []
        for job in jobs:
            table.append([
                job.id,
                job.job_type,
                job.status,
                job.started_at.strftime("%Y-%m-%d %H:%M") if job.started_at else "-",
                f"{job.records_processed}/{job.records_created}" if job.records_processed else "-",
                job.error_message[:50] + "..." if job.error_message and len(job.error_message) > 50 else job.error_message or ""
            ])
        
        print("\nRecent Scraper Jobs:")
        print(tabulate(table, headers=["ID", "Type", "Status", "Started", "Processed/Created", "Error"]))
        
    finally:
        session.close()


def cmd_stats(args):
    """Show database statistics"""
    session = get_session()
    
    try:
        stats = {
            "Superinvestors": session.query(Superinvestor).count(),
            "13F Filings": session.query(Filing13F).count(),
            "Holdings": session.query(Holding).count(),
            "Congress Members": session.query(CongressMember).count(),
            "Congress Trades": session.query(CongressTrade).count(),
            "Net Worth Reports": session.query(NetWorthReport).count(),
            "Scraper Jobs": session.query(ScraperJob).count(),
        }
        
        print("\nDatabase Statistics:")
        print("-" * 40)
        for key, value in stats.items():
            print(f"  {key}: {value:,}")
        
        # Recent activity
        print("\nRecent Activity:")
        print("-" * 40)
        
        latest_13f = session.query(Filing13F).order_by(Filing13F.filing_date.desc()).first()
        if latest_13f:
            print(f"  Latest 13F: {latest_13f.filing_date}")
        
        latest_trade = session.query(CongressTrade).order_by(CongressTrade.transaction_date.desc()).first()
        if latest_trade:
            print(f"  Latest Trade: {latest_trade.transaction_date}")
        
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(
        description="InvestorInsight Data Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # init-db
    parser_init = subparsers.add_parser('init-db', help='Initialize database')
    parser_init.set_defaults(func=cmd_init_db)
    
    # scrape
    parser_scrape = subparsers.add_parser('scrape', help='Run scrapers')
    parser_scrape.add_argument('type', choices=['13f', 'congress', 'networth', 'all'],
                              help='Type of scraper to run')
    parser_scrape.set_defaults(func=cmd_scrape)
    
    # add-investor
    parser_add = subparsers.add_parser('add-investor', help='Add a superinvestor to track')
    parser_add.add_argument('cik', help='SEC CIK number')
    parser_add.add_argument('--name', help='Investor name')
    parser_add.add_argument('--firm', help='Firm name')
    parser_add.add_argument('--scrape', action='store_true', help='Immediately scrape their filings')
    parser_add.set_defaults(func=cmd_add_investor)
    
    # status
    parser_status = subparsers.add_parser('status', help='Show scraper job status')
    parser_status.set_defaults(func=cmd_status)
    
    # stats
    parser_stats = subparsers.add_parser('stats', help='Show database statistics')
    parser_stats.set_defaults(func=cmd_stats)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == '__main__':
    main()
