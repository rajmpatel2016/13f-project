"""
InvestorInsight Data Seeder

Generates realistic sample data matching the exact structure that 
the SEC 13F and Congressional disclosure scrapers produce.

This allows the app to work immediately while the real scrapers
can be enabled in production with network access.

Run with: python3 seed_data.py
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
import random

DATA_DIR = Path("./data")
THIRTEENF_DIR = DATA_DIR / "13f"
CONGRESS_DIR = DATA_DIR / "congress"

# Ensure directories exist
THIRTEENF_DIR.mkdir(parents=True, exist_ok=True)
CONGRESS_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# SUPERINVESTOR DATA - Based on actual 13F filings
# =============================================================================

SUPERINVESTORS = {
    "1067983": {
        "name": "Warren Buffett",
        "firm": "Berkshire Hathaway Inc",
        "aum": "350B",
        "holdings": [
            {"ticker": "AAPL", "name": "Apple Inc", "value": 174300000, "shares": 915000000, "pct": 49.2},
            {"ticker": "BAC", "name": "Bank of America Corp", "value": 34800000, "shares": 1032852006, "pct": 9.8},
            {"ticker": "AXP", "name": "American Express Co", "value": 38500000, "shares": 151610700, "pct": 10.9},
            {"ticker": "KO", "name": "Coca-Cola Co", "value": 25200000, "shares": 400000000, "pct": 7.1},
            {"ticker": "CVX", "name": "Chevron Corp", "value": 17800000, "shares": 118610534, "pct": 5.0},
            {"ticker": "OXY", "name": "Occidental Petroleum", "value": 14200000, "shares": 264148432, "pct": 4.0},
            {"ticker": "KHC", "name": "Kraft Heinz Co", "value": 10800000, "shares": 325634818, "pct": 3.1},
            {"ticker": "MCO", "name": "Moody's Corp", "value": 10200000, "shares": 24669778, "pct": 2.9},
            {"ticker": "CB", "name": "Chubb Ltd", "value": 6800000, "shares": 27033784, "pct": 1.9},
            {"ticker": "DVA", "name": "DaVita Inc", "value": 5100000, "shares": 36095570, "pct": 1.4},
        ]
    },
    "1079114": {
        "name": "David Einhorn",
        "firm": "Greenlight Capital Inc",
        "aum": "2.1B",
        "holdings": [
            {"ticker": "TECK", "name": "Teck Resources Ltd", "value": 245000, "shares": 5200000, "pct": 12.8},
            {"ticker": "GTN", "name": "Gray Television Inc", "value": 189000, "shares": 35000000, "pct": 9.9},
            {"ticker": "GPRO", "name": "GoPro Inc", "value": 156000, "shares": 52000000, "pct": 8.2},
            {"ticker": "CNX", "name": "CNX Resources Corp", "value": 142000, "shares": 5800000, "pct": 7.4},
            {"ticker": "CHTR", "name": "Charter Communications", "value": 138000, "shares": 380000, "pct": 7.2},
            {"ticker": "BHF", "name": "Brighthouse Financial", "value": 125000, "shares": 2400000, "pct": 6.5},
            {"ticker": "KRTX", "name": "Karuna Therapeutics", "value": 118000, "shares": 350000, "pct": 6.2},
        ]
    },
    "1336528": {
        "name": "Bill Ackman",
        "firm": "Pershing Square Capital Management",
        "aum": "18B",
        "holdings": [
            {"ticker": "BN", "name": "Brookfield Corp", "value": 2100000, "shares": 43000000, "pct": 16.8},
            {"ticker": "CMG", "name": "Chipotle Mexican Grill", "value": 1850000, "shares": 31000000, "pct": 14.8},
            {"ticker": "HLT", "name": "Hilton Worldwide", "value": 1620000, "shares": 8900000, "pct": 13.0},
            {"ticker": "QSR", "name": "Restaurant Brands Intl", "value": 1580000, "shares": 23000000, "pct": 12.6},
            {"ticker": "GOOGL", "name": "Alphabet Inc", "value": 1420000, "shares": 8000000, "pct": 11.4},
            {"ticker": "HHH", "name": "Howard Hughes Holdings", "value": 980000, "shares": 13000000, "pct": 7.8},
            {"ticker": "NFLX", "name": "Netflix Inc", "value": 920000, "shares": 1500000, "pct": 7.4},
        ]
    },
    "1061768": {
        "name": "Seth Klarman",
        "firm": "Baupost Group LLC",
        "aum": "27B",
        "holdings": [
            {"ticker": "LPX", "name": "Louisiana-Pacific Corp", "value": 1850000, "shares": 18000000, "pct": 8.2},
            {"ticker": "VSAT", "name": "Viasat Inc", "value": 1420000, "shares": 62000000, "pct": 6.3},
            {"ticker": "FOXA", "name": "Fox Corp Class A", "value": 1380000, "shares": 42000000, "pct": 6.1},
            {"ticker": "EBAY", "name": "eBay Inc", "value": 1250000, "shares": 23000000, "pct": 5.5},
            {"ticker": "INTC", "name": "Intel Corp", "value": 1180000, "shares": 38000000, "pct": 5.2},
            {"ticker": "WBD", "name": "Warner Bros Discovery", "value": 980000, "shares": 95000000, "pct": 4.3},
        ]
    },
    "921669": {
        "name": "Carl Icahn",
        "firm": "Icahn Capital LP",
        "aum": "15B",
        "holdings": [
            {"ticker": "IEP", "name": "Icahn Enterprises LP", "value": 4200000, "shares": 298000000, "pct": 32.3},
            {"ticker": "CVR", "name": "CVR Energy Inc", "value": 1850000, "shares": 71000000, "pct": 14.2},
            {"ticker": "SWX", "name": "Southwest Gas Holdings", "value": 980000, "shares": 14000000, "pct": 7.5},
            {"ticker": "CVI", "name": "CVR Partners LP", "value": 620000, "shares": 5200000, "pct": 4.8},
            {"ticker": "XRX", "name": "Xerox Holdings Corp", "value": 580000, "shares": 38000000, "pct": 4.5},
        ]
    },
    "1649339": {
        "name": "Michael Burry",
        "firm": "Scion Asset Management LLC",
        "aum": "290M",
        "holdings": [
            {"ticker": "BABA", "name": "Alibaba Group", "value": 52000, "shares": 600000, "pct": 19.3},
            {"ticker": "JD", "name": "JD.com Inc", "value": 48000, "shares": 1500000, "pct": 17.8},
            {"ticker": "BIDU", "name": "Baidu Inc", "value": 38000, "shares": 400000, "pct": 14.1},
            {"ticker": "REAL", "name": "RealReal Inc", "value": 28000, "shares": 8500000, "pct": 10.4},
            {"ticker": "HCA", "name": "HCA Healthcare Inc", "value": 25000, "shares": 75000, "pct": 9.3},
            {"ticker": "OSCR", "name": "Oscar Health Inc", "value": 22000, "shares": 1200000, "pct": 8.1},
            {"ticker": "GOOG", "name": "Alphabet Inc Class C", "value": 18000, "shares": 100000, "pct": 6.7},
        ]
    },
    "1350694": {
        "name": "Ray Dalio",
        "firm": "Bridgewater Associates LP",
        "aum": "150B",
        "holdings": [
            {"ticker": "SPY", "name": "SPDR S&P 500 ETF Trust", "value": 8500000, "shares": 15000000, "pct": 8.9},
            {"ticker": "VWO", "name": "Vanguard FTSE Emerging Markets", "value": 7200000, "shares": 180000000, "pct": 7.5},
            {"ticker": "IVV", "name": "iShares Core S&P 500 ETF", "value": 6800000, "shares": 12000000, "pct": 7.1},
            {"ticker": "IEMG", "name": "iShares Core MSCI EM ETF", "value": 5400000, "shares": 110000000, "pct": 5.6},
            {"ticker": "PG", "name": "Procter & Gamble Co", "value": 4200000, "shares": 25000000, "pct": 4.4},
            {"ticker": "JNJ", "name": "Johnson & Johnson", "value": 3800000, "shares": 23000000, "pct": 4.0},
            {"ticker": "GOOGL", "name": "Alphabet Inc", "value": 3500000, "shares": 19000000, "pct": 3.7},
            {"ticker": "NVDA", "name": "NVIDIA Corp", "value": 3200000, "shares": 24000000, "pct": 3.3},
        ]
    },
    "1040273": {
        "name": "Dan Loeb",
        "firm": "Third Point LLC",
        "aum": "12B",
        "holdings": [
            {"ticker": "AMZN", "name": "Amazon.com Inc", "value": 980000, "shares": 5200000, "pct": 11.2},
            {"ticker": "PGR", "name": "Progressive Corp", "value": 850000, "shares": 3800000, "pct": 9.7},
            {"ticker": "META", "name": "Meta Platforms Inc", "value": 780000, "shares": 1500000, "pct": 8.9},
            {"ticker": "MSFT", "name": "Microsoft Corp", "value": 720000, "shares": 1700000, "pct": 8.2},
            {"ticker": "NVDA", "name": "NVIDIA Corp", "value": 680000, "shares": 5100000, "pct": 7.8},
            {"ticker": "DHR", "name": "Danaher Corp", "value": 620000, "shares": 2400000, "pct": 7.1},
        ]
    },
    "1536411": {
        "name": "Stanley Druckenmiller",
        "firm": "Duquesne Family Office LLC",
        "aum": "3B",
        "holdings": [
            {"ticker": "NVDA", "name": "NVIDIA Corp", "value": 420000, "shares": 3200000, "pct": 15.6},
            {"ticker": "MSFT", "name": "Microsoft Corp", "value": 380000, "shares": 900000, "pct": 14.1},
            {"ticker": "GOOGL", "name": "Alphabet Inc", "value": 320000, "shares": 1800000, "pct": 11.9},
            {"ticker": "AMZN", "name": "Amazon.com Inc", "value": 280000, "shares": 1500000, "pct": 10.4},
            {"ticker": "META", "name": "Meta Platforms Inc", "value": 250000, "shares": 480000, "pct": 9.3},
            {"ticker": "NFLX", "name": "Netflix Inc", "value": 180000, "shares": 280000, "pct": 6.7},
        ]
    },
    "1510387": {
        "name": "Joel Greenblatt",
        "firm": "Gotham Asset Management LLC",
        "aum": "5.4B",
        "holdings": [
            {"ticker": "MSFT", "name": "Microsoft Corp", "value": 420000, "shares": 1000000, "pct": 9.2},
            {"ticker": "AAPL", "name": "Apple Inc", "value": 380000, "shares": 2000000, "pct": 8.3},
            {"ticker": "GOOGL", "name": "Alphabet Inc", "value": 350000, "shares": 2000000, "pct": 7.7},
            {"ticker": "META", "name": "Meta Platforms Inc", "value": 320000, "shares": 600000, "pct": 7.0},
            {"ticker": "AMZN", "name": "Amazon.com Inc", "value": 290000, "shares": 1500000, "pct": 6.4},
            {"ticker": "NVDA", "name": "NVIDIA Corp", "value": 280000, "shares": 2100000, "pct": 6.1},
            {"ticker": "V", "name": "Visa Inc", "value": 250000, "shares": 900000, "pct": 5.5},
        ]
    },
}


# =============================================================================
# CONGRESSIONAL DATA - Based on actual STOCK Act filings
# =============================================================================

CONGRESS_MEMBERS = {
    "P000197": {
        "name": "Nancy Pelosi",
        "first_name": "Nancy",
        "last_name": "Pelosi",
        "party": "D",
        "chamber": "House",
        "state": "CA",
        "district": "11",
        "committees": ["Intelligence"],
        "net_worth": "$272.5M",
        "trades_2024": 17,
        "volume_2024": "$37.75M"
    },
    "G000583": {
        "name": "Josh Gottheimer",
        "first_name": "Josh",
        "last_name": "Gottheimer",
        "party": "D",
        "chamber": "House",
        "state": "NJ",
        "district": "5",
        "committees": ["Financial Services", "Homeland Security"],
        "net_worth": "$25.6M",
        "trades_2024": 526,
        "volume_2024": "$91.05M"
    },
    "T000278": {
        "name": "Tommy Tuberville",
        "first_name": "Tommy",
        "last_name": "Tuberville",
        "party": "R",
        "chamber": "Senate",
        "state": "AL",
        "district": None,
        "committees": ["Armed Services", "Agriculture", "Veterans' Affairs"],
        "net_worth": "$12.8M",
        "trades_2024": 202,
        "volume_2024": "$5.53M"
    },
    "C001123": {
        "name": "Gilbert Cisneros",
        "first_name": "Gilbert",
        "last_name": "Cisneros",
        "party": "D",
        "chamber": "House",
        "state": "CA",
        "district": "31",
        "committees": ["Armed Services", "Veterans' Affairs"],
        "net_worth": "$68.2M",
        "trades_2024": 120,
        "volume_2024": "$4.2M"
    },
    "C001120": {
        "name": "Dan Crenshaw",
        "first_name": "Dan",
        "last_name": "Crenshaw",
        "party": "R",
        "chamber": "House",
        "state": "TX",
        "district": "2",
        "committees": ["Energy & Commerce", "Intelligence"],
        "net_worth": "$2.1M",
        "trades_2024": 45,
        "volume_2024": "$1.8M"
    },
    "M001157": {
        "name": "Michael McCaul",
        "first_name": "Michael",
        "last_name": "McCaul",
        "party": "R",
        "chamber": "House",
        "state": "TX",
        "district": "10",
        "committees": ["Foreign Affairs", "Homeland Security"],
        "net_worth": "$113.3M",
        "trades_2024": 22,
        "volume_2024": "$2.1M"
    },
    "F000472": {
        "name": "Scott Franklin",
        "first_name": "Scott",
        "last_name": "Franklin",
        "party": "R",
        "chamber": "House",
        "state": "FL",
        "district": "18",
        "committees": ["Appropriations", "AI Task Force"],
        "net_worth": "$25.6M",
        "trades_2024": 69,
        "volume_2024": "$5.99M"
    },
    "M001190": {
        "name": "Markwayne Mullin",
        "first_name": "Markwayne",
        "last_name": "Mullin",
        "party": "R",
        "chamber": "Senate",
        "state": "OK",
        "district": None,
        "committees": ["Armed Services", "Environment", "Indian Affairs"],
        "net_worth": "$31.6M",
        "trades_2024": 71,
        "volume_2024": "$4.41M"
    },
    "S001217": {
        "name": "Rick Scott",
        "first_name": "Rick",
        "last_name": "Scott",
        "party": "R",
        "chamber": "Senate",
        "state": "FL",
        "district": None,
        "committees": ["Armed Services", "Budget", "Commerce"],
        "net_worth": "$548.8M",
        "trades_2024": 15,
        "volume_2024": "$3.2M"
    },
    "B001236": {
        "name": "John Boozman",
        "first_name": "John",
        "last_name": "Boozman",
        "party": "R",
        "chamber": "Senate",
        "state": "AR",
        "district": None,
        "committees": ["Agriculture", "Appropriations"],
        "net_worth": "$3.8M",
        "trades_2024": 42,
        "volume_2024": "$890K"
    },
    "J000309": {
        "name": "Jonathan Jackson",
        "first_name": "Jonathan",
        "last_name": "Jackson",
        "party": "D",
        "chamber": "House",
        "state": "IL",
        "district": "1",
        "committees": ["Foreign Affairs", "Agriculture"],
        "net_worth": "$1.2M",
        "trades_2024": 35,
        "volume_2024": "$850K"
    },
    "D000617": {
        "name": "Suzan DelBene",
        "first_name": "Suzan",
        "last_name": "DelBene",
        "party": "D",
        "chamber": "House",
        "state": "WA",
        "district": "1",
        "committees": ["Ways & Means"],
        "net_worth": "$79.4M",
        "trades_2024": 33,
        "volume_2024": "$2.8M"
    },
    "W000779": {
        "name": "Ron Wyden",
        "first_name": "Ron",
        "last_name": "Wyden",
        "party": "D",
        "chamber": "Senate",
        "state": "OR",
        "district": None,
        "committees": ["Finance", "Budget", "Intelligence"],
        "net_worth": "$8.9M",
        "trades_2024": 19,
        "volume_2024": "$2.1M"
    },
    "K000389": {
        "name": "Ro Khanna",
        "first_name": "Ro",
        "last_name": "Khanna",
        "party": "D",
        "chamber": "House",
        "state": "CA",
        "district": "17",
        "committees": ["Armed Services", "Oversight"],
        "net_worth": "$27.8M",
        "trades_2024": 18,
        "volume_2024": "$1.5M"
    },
    "M000355": {
        "name": "Mitch McConnell",
        "first_name": "Mitch",
        "last_name": "McConnell",
        "party": "R",
        "chamber": "Senate",
        "state": "KY",
        "district": None,
        "committees": ["Appropriations", "Agriculture", "Rules"],
        "net_worth": "$35.2M",
        "trades_2024": 8,
        "volume_2024": "$450K"
    },
}

# Stock tickers for generating trades
TRADED_STOCKS = {
    "tech": ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "AVGO", "AMD", "INTC", "CRM", "ORCL", "PLTR", "PANW"],
    "defense": ["RTX", "LMT", "NOC", "BA", "GD", "HII", "LHX", "AVAV"],
    "energy": ["XOM", "CVX", "COP", "SLB", "HAL", "OXY", "EOG"],
    "finance": ["JPM", "BAC", "GS", "MS", "C", "WFC", "SCHW", "BLK", "V", "MA"],
    "healthcare": ["UNH", "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY"],
    "other": ["TSLA", "NFLX", "DIS", "NKE", "COST", "HD", "WMT", "TGT"]
}

AMOUNT_RANGES = [
    ("$1,001 - $15,000", 1001, 15000),
    ("$15,001 - $50,000", 15001, 50000),
    ("$50,001 - $100,000", 50001, 100000),
    ("$100,001 - $250,000", 100001, 250000),
    ("$250,001 - $500,000", 250001, 500000),
    ("$500,001 - $1,000,000", 500001, 1000000),
    ("$1,000,001 - $5,000,000", 1000001, 5000000),
]


def generate_superinvestor_data():
    """Generate 13F holdings data in the exact format the scraper produces"""
    filings = {}
    
    for cik, investor in SUPERINVESTORS.items():
        holdings = []
        total_value = 0
        
        for h in investor["holdings"]:
            holding = {
                "cusip": f"{random.randint(100000, 999999)}10",  # Fake CUSIP
                "issuer_name": h["name"],
                "class_title": "COM",
                "value": h["value"],
                "shares": h["shares"],
                "share_type": "SH",
                "investment_discretion": "SOLE",
                "voting_authority_sole": h["shares"],
                "voting_authority_shared": 0,
                "voting_authority_none": 0,
                "ticker": h["ticker"],
                "pct_portfolio": h["pct"]
            }
            holdings.append(holding)
            total_value += h["value"]
        
        filings[cik] = {
            "cik": cik,
            "accession_number": f"0001067983-24-{random.randint(100000, 999999)}",
            "filing_date": "2024-11-14",
            "report_date": "2024-09-30",
            "investor_name": investor["name"],
            "firm_name": investor["firm"],
            "total_value": total_value,
            "holdings": holdings
        }
    
    data = {
        "last_updated": datetime.now().isoformat(),
        "filings": filings
    }
    
    filepath = THIRTEENF_DIR / "superinvestor_holdings.json"
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Generated 13F data for {len(filings)} superinvestors")
    return data


def generate_congress_trades():
    """Generate congressional trading data in the exact format the scraper produces"""
    transactions = []
    
    # Generate trades for each member
    for member_id, member in CONGRESS_MEMBERS.items():
        # Number of trades based on their actual activity level
        num_trades = random.randint(5, 30)
        
        # Bias stock selection based on committees
        stock_pool = list(TRADED_STOCKS["tech"]) + list(TRADED_STOCKS["other"])
        
        if any("Armed" in c or "Foreign" in c for c in member["committees"]):
            stock_pool += TRADED_STOCKS["defense"] * 3  # Weight defense stocks
        if any("Energy" in c for c in member["committees"]):
            stock_pool += TRADED_STOCKS["energy"] * 3
        if any("Financial" in c or "Banking" in c for c in member["committees"]):
            stock_pool += TRADED_STOCKS["finance"] * 3
        if any("Intel" in c or "AI" in c for c in member["committees"]):
            stock_pool += TRADED_STOCKS["tech"] * 2
        
        for i in range(num_trades):
            # Random date in the past 90 days
            days_ago = random.randint(1, 90)
            txn_date = datetime.now() - timedelta(days=days_ago)
            filing_date = txn_date + timedelta(days=random.randint(5, 45))
            
            ticker = random.choice(stock_pool)
            txn_type = random.choice(["Purchase", "Sale", "Sale (Partial)", "Sale (Full)"])
            amount = random.choice(AMOUNT_RANGES)
            
            # Higher amounts for wealthier members
            if "Pelosi" in member["name"] or "Scott" in member["name"] or "McCaul" in member["name"]:
                amount = random.choice(AMOUNT_RANGES[3:])  # Higher amounts
            
            transaction = {
                "member_id": member_id,
                "member_name": member["name"],
                "party": member["party"],
                "chamber": member["chamber"],
                "state": member["state"],
                "transaction_date": txn_date.strftime("%Y-%m-%d"),
                "filing_date": filing_date.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "asset_name": f"{ticker} - Common Stock",
                "asset_type": "Stock",
                "transaction_type": txn_type,
                "amount_range": amount[0],
                "amount_min": amount[1],
                "amount_max": amount[2],
                "owner": random.choice(["Self", "Spouse", "Joint"]),
                "committees": member["committees"],
                "filing_url": f"https://efdsearch.senate.gov/search/view/paper/{random.randint(10000, 99999)}/"
            }
            transactions.append(transaction)
    
    # Sort by transaction date, most recent first
    transactions.sort(key=lambda t: t["transaction_date"], reverse=True)
    
    data = {
        "last_updated": datetime.now().isoformat(),
        "total_transactions": len(transactions),
        "transactions": transactions
    }
    
    filepath = CONGRESS_DIR / "all_congressional_trades.json"
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Generated {len(transactions)} congressional transactions")
    return data


def generate_member_files():
    """Generate individual member JSON files"""
    for member_id, member in CONGRESS_MEMBERS.items():
        filepath = CONGRESS_DIR / f"transactions_{member_id}.json"
        
        # Get transactions for this member
        with open(CONGRESS_DIR / "all_congressional_trades.json") as f:
            all_data = json.load(f)
        
        member_txns = [t for t in all_data["transactions"] if t["member_id"] == member_id]
        
        data = {
            "member": member,
            "last_updated": datetime.now().isoformat(),
            "transactions": member_txns
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    print(f"Generated individual files for {len(CONGRESS_MEMBERS)} members")


def main():
    """Seed all data"""
    print("=" * 60)
    print("InvestorInsight Data Seeder")
    print("=" * 60)
    print()
    
    print("Generating superinvestor 13F data...")
    generate_superinvestor_data()
    print()
    
    print("Generating congressional trading data...")
    generate_congress_trades()
    print()
    
    print("Generating individual member files...")
    generate_member_files()
    print()
    
    print("=" * 60)
    print("Data seeding complete!")
    print(f"Data directory: {DATA_DIR.absolute()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
