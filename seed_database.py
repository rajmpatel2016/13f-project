#!/usr/bin/env python3
"""
Seed the SQLite database with sample data from the frontend.
This allows the app to work immediately without running scrapers.

Run with: python seed_database.py
"""
import sys
import os
from datetime import date, datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, get_session
from database.models import (
    Superinvestor, Filing13F, Holding,
    CongressMember, CongressTrade, NetWorthReport, NetWorthAsset, NetWorthLiability
)


def seed_superinvestors(session):
    """Seed superinvestors and their holdings"""
    
    # Same data as frontend sample
    investors = [
        ("1", "Capital Research", "Capital Group", 2100000000000),
        ("2", "Fidelity", "FMR LLC", 1200000000000),
        ("3", "T Rowe Price", "T. Rowe Price", 780000000000),
        ("1067983", "Warren Buffett", "Berkshire Hathaway", 267334499000),
        ("4", "Primecap Management", "Primecap Management", 156000000000),
        ("5", "Dodge & Cox", "Dodge & Cox", 118500000000),
        ("6", "First Eagle Investment", "First Eagle Investment Management", 54800000000),
        ("7", "Chris Hohn", "TCI Fund Management", 52700000000),
        ("8", "Bill & Melinda Gates Foundation", "Gates Foundation Trust", 36600000000),
        ("9", "Chase Coleman", "Tiger Global Management", 32400000000),
        ("10", "Polen Capital", "Polen Capital Management", 30800000000),
        ("11", "Terry Smith", "Fundsmith", 19800000000),
        ("12", "Christopher Davis", "Davis Advisors", 19200000000),
        ("13", "David Herro", "Harris Associates", 18900000000),
        ("14", "Duan Yongping", "H&H International Investment", 14700000000),
        ("1336528", "Bill Ackman", "Pershing Square Capital", 14600000000),
        ("15", "Stephen Mandel", "Lone Pine Capital", 13700000000),
        ("16", "Thomas Gayner", "Markel Group", 12300000000),
        ("17", "Chuck Akre", "Akre Capital Management", 10000000000),
        ("18", "John Armitage", "Egerton Capital", 9480000000),
        ("19", "Thomas Russo", "Gardner Russo & Quinn", 9330000000),
        ("20", "Carl Icahn", "Icahn Capital Management", 9140000000),
        ("21", "Daniel Loeb", "Third Point", 8300000000),
        ("22", "Jensen Investment", "Jensen Investment Management", 8060000000),
        ("23", "Sarah Ketterer", "Causeway Capital Management", 7550000000),
        ("24", "Lee Ainslie", "Maverick Capital", 7470000000),
        ("1656456", "David Tepper", "Appaloosa Management", 7380000000),
        ("25", "Bill Nygren", "Oakmark Select Fund", 7190000000),
        ("26", "AKO Capital", "AKO Capital", 7030000000),
        ("27", "Greenhaven Associates", "Greenhaven Associates", 6580000000),
        ("28", "Steven Romick", "FPA Crescent Fund", 6280000000),
        ("29", "David Abrams", "Abrams Capital Management", 6170000000),
        ("30", "Mairs & Power", "Mairs & Power Growth Fund", 5670000000),
        ("31", "AltaRock Partners", "AltaRock Partners", 5470000000),
        ("1061768", "Seth Klarman", "Baupost Group", 4760000000),
        ("32", "Howard Marks", "Oaktree Capital Management", 4740000000),
        ("33", "Glenn Greenberg", "Brave Warrior Advisors", 4310000000),
        ("34", "Donald Yacktman", "Yacktman Asset Management", 4200000000),
        ("35", "Nelson Peltz", "Trian Fund Management", 4110000000),
        ("36", "Lindsell Train", "Lindsell Train", 4080000000),
        ("37", "Ruane Cunniff", "Sequoia Fund", 3690000000),
        ("38", "Li Lu", "Himalaya Capital Management", 3230000000),
        ("39", "Leon Cooperman", "Omega Advisors", 3200000000),
        ("40", "Harry Burn", "Sound Shore", 3030000000),
        ("41", "Francois Rochon", "Giverny Capital", 2950000000),
        ("42", "Tweedy Browne", "Tweedy Browne Company", 2900000000),
        ("43", "David Einhorn", "Greenlight Capital", 2540000000),
        ("44", "Samantha McLemore", "Patient Capital Management", 2500000000),
        ("45", "Clifford Sosin", "CAS Investment Partners", 2240000000),
        ("46", "Wally Weitz", "Weitz Investment Management", 2100000000),
        ("47", "Prem Watsa", "Fairfax Financial Holdings", 2070000000),
        ("48", "Bruce Berkowitz", "Fairholme Capital", 1240000000),
        ("49", "Pat Dorsey", "Dorsey Asset Management", 1110000000),
        ("50", "David Katz", "Matrix Asset Advisors", 1080000000),
        ("51", "Mason Hawkins", "Longleaf Partners", 960000000),
        ("52", "FPA Queens Road", "FPA Queens Road Small Cap Value", 960000000),
        ("53", "Arnold Van Den Berg", "Century Management", 890000000),
        ("54", "John Rogers", "Ariel Appreciation Fund", 884000000),
        ("55", "Christopher Bloomstran", "Semper Augustus", 801000000),
        ("56", "Dennis Hong", "ShawSpring Partners", 673000000),
        ("57", "Greg Alexander", "Conifer Management", 631000000),
        ("58", "Richard Pzena", "Hancock Classic Value", 591000000),
        ("59", "David Rolfe", "Wedgewood Partners", 548000000),
        ("60", "Meridian Contrarian", "Meridian Contrarian Fund", 547000000),
        ("61", "Robert Vinall", "RV Capital GmbH", 546000000),
        ("62", "Third Avenue Management", "Third Avenue Management", 533000000),
        ("63", "Kahn Brothers Group", "Kahn Brothers Group", 532000000),
        ("64", "Robert Olstein", "Olstein Capital Management", 530000000),
        ("65", "Glenn Welling", "Engaged Capital", 408000000),
        ("66", "Josh Tarasoff", "Greenlea Lane Capital", 343000000),
        ("67", "Mohnish Pabrai", "Pabrai Investments", 337000000),
        ("68", "Norbert Lou", "Punch Card Management", 322000000),
        ("69", "Guy Spier", "Aquamarine Capital", 317000000),
        ("70", "Bill Miller", "Miller Value Partners", 272000000),
        ("71", "Bryan Lawrence", "Oakcliff Capital", 232000000),
        ("72", "Francis Chou", "Chou Associates", 208000000),
        ("73", "Tom Bancroft", "Makaira Partners", 195000000),
        ("74", "Alex Roepers", "Atlantic Investment Management", 174000000),
        ("75", "Hillman Value Fund", "Hillman Value Fund", 101000000),
        ("76", "Charles Bobrinskoy", "Ariel Focus Fund", 75000000),
        ("1649339", "Michael Burry", "Scion Asset Management", 55000000),
    ]
    
    # Sample holdings data for key investors
    holdings_data = {
        "1067983": [  # Warren Buffett
            ("AAPL", "Apple Inc", 49.2, 91000000000, 905000000),
            ("BAC", "Bank of America", 10.5, 28100000000, 1033000000),
            ("AXP", "American Express", 8.1, 21700000000, 152000000),
            ("KO", "Coca-Cola Co", 7.3, 19600000000, 400000000),
            ("CVX", "Chevron Corp", 6.0, 16100000000, 126000000),
            ("OXY", "Occidental Petroleum", 5.0, 13400000000, 248000000),
            ("KHC", "Kraft Heinz Co", 4.0, 10700000000, 326000000),
            ("MCO", "Moody's Corp", 3.4, 9100000000, 25000000),
        ],
        "1336528": [  # Bill Ackman
            ("BN", "Brookfield Corp", 28.2, 3500000000, 68000000),
            ("GOOG", "Alphabet Inc", 25.0, 3100000000, 2200000),
            ("HLT", "Hilton Worldwide", 22.6, 2800000000, 14000000),
            ("CMG", "Chipotle", 15.0, 1860000000, 3600000),
            ("NKE", "Nike Inc", 9.2, 1140000000, 13000000),
        ],
        "1649339": [  # Michael Burry
            ("BABA", "Alibaba Group", 25.5, 14000000, 160000),
            ("JD", "JD.com Inc", 22.2, 54000000, 200000),
            ("GOOG", "Alphabet Inc", 17.3, 42000000, 30000),
            ("BKNG", "Booking Holdings", 15.0, 36000000, 8000),
            ("HCA", "HCA Healthcare", 12.0, 29000000, 11000),
        ],
        "1656456": [  # David Tepper
            ("NVDA", "NVIDIA Corp", 26.5, 1800000000, 4000000),
            ("META", "Meta Platforms", 17.6, 1200000000, 2500000),
            ("MSFT", "Microsoft Corp", 14.4, 980000000, 2600000),
            ("AMZN", "Amazon.com", 12.5, 850000000, 5000000),
            ("GOOG", "Alphabet Inc", 10.6, 720000000, 5200000),
            ("AMD", "AMD Inc", 7.9, 540000000, 4000000),
        ],
        "1061768": [  # Seth Klarman
            ("LBTYA", "Liberty Global", 22.0, 1800000000, 95000000),
            ("VSAT", "Viasat Inc", 14.6, 1200000000, 45000000),
            ("EBAY", "eBay Inc", 12.0, 980000000, 18000000),
            ("INTC", "Intel Corp", 10.4, 850000000, 28000000),
            ("WBD", "Warner Bros Discovery", 8.8, 720000000, 65000000),
        ],
    }
    
    count = 0
    for cik, name, firm, value in investors:
        investor = Superinvestor(cik=cik, name=name, firm=firm)
        session.add(investor)
        session.flush()
        
        # Create filing
        filing = Filing13F(
            superinvestor_id=investor.id,
            accession_number=f"0001-{cik}-2024Q4",
            filing_date=date(2025, 1, 10),
            report_date=date(2024, 12, 31),
            total_value=value,
            positions_count=len(holdings_data.get(cik, []))
        )
        session.add(filing)
        session.flush()
        
        # Add holdings if we have them
        if cik in holdings_data:
            for ticker, issuer, pct, val, shares in holdings_data[cik]:
                holding = Holding(
                    superinvestor_id=investor.id,
                    filing_id=filing.id,
                    ticker=ticker,
                    issuer_name=issuer,
                    pct_portfolio=pct,
                    value=val,
                    shares=shares,
                    is_new=False,
                    is_sold=False
                )
                session.add(holding)
        
        count += 1
    
    session.commit()
    print(f"  Seeded {count} superinvestors")


def seed_congress_members(session):
    """Seed congress members and their trades"""
    
    # Key members with trades
    members = [
        ("P000197", "Nancy Pelosi", "D", "House", "CA"),
        ("T000278", "Tommy Tuberville", "R", "Senate", "AL"),
        ("C001120", "Dan Crenshaw", "R", "House", "TX"),
        ("G000583", "Josh Gottheimer", "D", "House", "NJ"),
        ("M001157", "Michael McCaul", "R", "House", "TX"),
        ("W000805", "Mark Warner", "D", "Senate", "VA"),
        ("S001217", "Rick Scott", "R", "Senate", "FL"),
        ("M001190", "Markwayne Mullin", "R", "Senate", "OK"),
        ("H001086", "Bill Hagerty", "R", "Senate", "TN"),
        ("S000148", "Chuck Schumer", "D", "Senate", "NY"),
        ("W000817", "Elizabeth Warren", "D", "Senate", "MA"),
        ("R000595", "Marco Rubio", "R", "Senate", "FL"),
        ("C001098", "Ted Cruz", "R", "Senate", "TX"),
        ("O000172", "Alexandria Ocasio-Cortez", "D", "House", "NY"),
        ("G000596", "Marjorie Taylor Greene", "R", "House", "GA"),
    ]
    
    # Sample trades
    trades_data = {
        "P000197": [
            ("NVDA", "NVIDIA Corp", "Purchase", "$1M - $5M", "2024-12-15"),
            ("GOOGL", "Alphabet Inc", "Purchase", "$500K - $1M", "2024-11-20"),
            ("AAPL", "Apple Inc", "Sale", "$1M - $5M", "2024-10-05"),
            ("TSLA", "Tesla Inc", "Sale", "$500K - $1M", "2024-09-15"),
            ("MSFT", "Microsoft Corp", "Purchase", "$250K - $500K", "2024-08-20"),
        ],
        "T000278": [
            ("MSFT", "Microsoft Corp", "Purchase", "$100K - $250K", "2024-12-10"),
            ("AAPL", "Apple Inc", "Sale", "$50K - $100K", "2024-11-28"),
            ("NVDA", "NVIDIA Corp", "Purchase", "$250K - $500K", "2024-11-15"),
            ("META", "Meta Platforms", "Sale", "$100K - $250K", "2024-10-20"),
            ("AMZN", "Amazon.com", "Purchase", "$50K - $100K", "2024-09-25"),
        ],
        "C001120": [
            ("MSFT", "Microsoft Corp", "Purchase", "$50K - $100K", "2024-12-01"),
            ("AMZN", "Amazon.com", "Purchase", "$100K - $250K", "2024-11-10"),
            ("TSLA", "Tesla Inc", "Sale", "$50K - $100K", "2024-10-25"),
            ("XOM", "Exxon Mobil", "Purchase", "$15K - $50K", "2024-09-15"),
        ],
        "G000583": [
            ("GOOGL", "Alphabet Inc", "Purchase", "$100K - $250K", "2024-12-05"),
            ("META", "Meta Platforms", "Sale", "$50K - $100K", "2024-11-22"),
            ("CRM", "Salesforce", "Purchase", "$50K - $100K", "2024-11-01"),
        ],
        "M001157": [
            ("AAPL", "Apple Inc", "Sale", "$500K - $1M", "2024-12-08"),
            ("NVDA", "NVIDIA Corp", "Purchase", "$250K - $500K", "2024-11-18"),
            ("INTC", "Intel Corp", "Purchase", "$100K - $250K", "2024-10-30"),
        ],
    }
    
    # Net worth data
    networth_data = {
        "P000197": (117000000, 257000000, "Paul Pelosi"),
        "W000805": (90000000, 200000000, None),
        "M001157": (75000000, 150000000, None),
        "S001217": (50000000, 100000000, None),
    }
    
    member_count = 0
    trade_count = 0
    
    for bioguide_id, name, party, chamber, state in members:
        member = CongressMember(
            bioguide_id=bioguide_id,
            name=name,
            party=party,
            chamber=chamber,
            state=state,
            is_active=True
        )
        session.add(member)
        session.flush()
        member_count += 1
        
        # Add trades
        if bioguide_id in trades_data:
            for i, (ticker, asset_name, txn_type, amount, txn_date) in enumerate(trades_data[bioguide_id]):
                trade = CongressTrade(
                    member_id=member.id,
                    transaction_date=datetime.strptime(txn_date, "%Y-%m-%d").date(),
                    ticker=ticker,
                    asset_name=asset_name,
                    transaction_type=txn_type,
                    amount_range_text=amount,
                    ptr_id=f"{bioguide_id}-{txn_date}-{i}"
                )
                session.add(trade)
                trade_count += 1
        
        # Add net worth
        if bioguide_id in networth_data:
            nw_min, nw_max, spouse = networth_data[bioguide_id]
            report = NetWorthReport(
                member_id=member.id,
                report_year=2024,
                net_worth_min=nw_min,
                net_worth_max=nw_max,
                total_assets_min=nw_min + 5000000,
                total_assets_max=nw_max + 10000000,
                total_liabilities_min=1000000,
                total_liabilities_max=5000000,
                spouse_name=spouse
            )
            session.add(report)
    
    session.commit()
    print(f"  Seeded {member_count} congress members")
    print(f"  Seeded {trade_count} trades")


def main():
    print("Initializing database...")
    init_db()
    
    session = get_session()
    
    try:
        # Check if already seeded
        if session.query(Superinvestor).count() > 0:
            print("Database already has data. Skipping seed.")
            print(f"  Superinvestors: {session.query(Superinvestor).count()}")
            print(f"  Congress members: {session.query(CongressMember).count()}")
            return
        
        print("\nSeeding superinvestors...")
        seed_superinvestors(session)
        
        print("\nSeeding congress members...")
        seed_congress_members(session)
        
        print("\nDone! Database seeded successfully.")
        print(f"\nStats:")
        print(f"  Superinvestors: {session.query(Superinvestor).count()}")
        print(f"  Filings: {session.query(Filing13F).count()}")
        print(f"  Holdings: {session.query(Holding).count()}")
        print(f"  Congress members: {session.query(CongressMember).count()}")
        print(f"  Trades: {session.query(CongressTrade).count()}")
        print(f"  Net worth reports: {session.query(NetWorthReport).count()}")
        
    finally:
        session.close()


if __name__ == "__main__":
    main()
