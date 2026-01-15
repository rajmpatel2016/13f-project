"""
Congressional STOCK Act Disclosure Scraper for InvestorInsight

Scrapes periodic transaction reports (PTRs) from:
- House Financial Disclosures: https://disclosures-clerk.house.gov/
- Senate Financial Disclosures: https://efdsearch.senate.gov/

The STOCK Act requires members of Congress to report securities transactions
over $1,000 within 45 days of the transaction.
"""

import requests
import json
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin, urlencode
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Endpoints
HOUSE_DISCLOSURES_BASE = "https://disclosures-clerk.house.gov"
HOUSE_DISCLOSURES_SEARCH = f"{HOUSE_DISCLOSURES_BASE}/FinancialDisclosure"
HOUSE_PTR_SEARCH = f"{HOUSE_DISCLOSURES_BASE}/PublicDisclosure/FinancialDisclosure/ViewMemberSearchResult"

SENATE_DISCLOSURES_BASE = "https://efdsearch.senate.gov"
SENATE_SEARCH_API = f"{SENATE_DISCLOSURES_BASE}/search/"
SENATE_REPORTS_API = f"{SENATE_DISCLOSURES_BASE}/search/report/data/"

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


@dataclass
class CongressMember:
    """Represents a member of Congress"""
    bioguide_id: str
    name: str
    first_name: str
    last_name: str
    party: str  # D, R, I
    chamber: str  # House, Senate
    state: str
    district: Optional[str] = None
    committees: List[str] = field(default_factory=list)
    
    @property
    def full_state_district(self) -> str:
        if self.district:
            return f"{self.state}-{self.district}"
        return self.state


@dataclass
class Asset:
    """Represents an asset from Annual Financial Disclosure"""
    category: str  # Real Estate, Stocks, Business Interest, Retirement, Cash, Other
    description: str
    value_min: int
    value_max: int
    income_type: Optional[str] = None  # Dividends, Interest, Capital Gains, Rent, etc.
    income_min: Optional[int] = None
    income_max: Optional[int] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class Liability:
    """Represents a liability from Annual Financial Disclosure"""
    description: str
    creditor: Optional[str]
    value_min: int
    value_max: int
    interest_rate: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass 
class AnnualFinancialDisclosure:
    """
    Represents a full Annual Financial Disclosure (AFD) filing.
    This is separate from PTRs and contains net worth information.
    
    AFDs include:
    - Schedule A: Assets (over $1,000)
    - Schedule B: Transactions (already covered by PTRs)
    - Schedule C: Earned Income
    - Schedule D: Liabilities (over $10,000)
    - Schedule E: Positions
    - Schedule F: Agreements
    - Schedule G: Gifts
    - Schedule H: Travel Reimbursements
    """
    member_id: str
    member_name: str
    party: str
    chamber: str
    state: str
    filing_year: int
    filing_date: str
    filing_url: Optional[str]
    
    # Spouse info
    spouse_name: Optional[str] = None
    
    # Schedule A: Assets
    assets: List[Asset] = field(default_factory=list)
    
    # Schedule D: Liabilities  
    liabilities: List[Liability] = field(default_factory=list)
    
    # Calculated totals
    total_assets_min: int = 0
    total_assets_max: int = 0
    total_liabilities_min: int = 0
    total_liabilities_max: int = 0
    net_worth_min: int = 0
    net_worth_max: int = 0
    
    # Income info
    income_sources: List[str] = field(default_factory=list)
    
    def calculate_totals(self):
        """Calculate net worth from assets and liabilities"""
        self.total_assets_min = sum(a.value_min for a in self.assets)
        self.total_assets_max = sum(a.value_max for a in self.assets)
        self.total_liabilities_min = sum(l.value_min for l in self.liabilities)
        self.total_liabilities_max = sum(l.value_max for l in self.liabilities)
        self.net_worth_min = self.total_assets_min - self.total_liabilities_max
        self.net_worth_max = self.total_assets_max - self.total_liabilities_min
    
    def to_dict(self):
        return {
            **{k: v for k, v in asdict(self).items() if k not in ['assets', 'liabilities']},
            'assets': [a.to_dict() for a in self.assets],
            'liabilities': [l.to_dict() for l in self.liabilities],
        }


@dataclass
class StockTransaction:
    """Represents a single stock transaction from a PTR"""
    member_id: str
    member_name: str
    party: str
    chamber: str
    state: str
    
    transaction_date: str
    filing_date: str
    
    ticker: Optional[str]
    asset_name: str
    asset_type: str  # Stock, Stock Option, Bond, etc.
    
    transaction_type: str  # Purchase, Sale, Sale (Partial), Sale (Full), Exchange
    amount_range: str  # e.g., "$1,001 - $15,000"
    amount_min: int
    amount_max: int
    
    owner: str  # Self, Spouse, Joint, Dependent Child
    
    # Optional fields
    cap_gains_over_200: Optional[bool] = None
    committees: List[str] = field(default_factory=list)
    filing_url: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class PeriodicTransactionReport:
    """Represents a full PTR filing"""
    report_id: str
    member_id: str
    member_name: str
    filing_date: str
    report_year: int
    chamber: str
    filing_url: str
    transactions: List[StockTransaction]
    
    def to_dict(self):
        return {
            **{k: v for k, v in asdict(self).items() if k != 'transactions'},
            'transactions': [t.to_dict() for t in self.transactions]
        }


# Current Congress Members (119th Congress) - Full list imported from data file
# This is a subset for quick reference - full list in data/congress_members_full.py
try:
    from data.congress_members_full import ALL_CONGRESS_MEMBERS as CONGRESS_MEMBERS
except ImportError:
    # Fallback to inline definition if import fails
    CONGRESS_MEMBERS = {
        # House Members - Top Traders
        "P000197": CongressMember("P000197", "Nancy Pelosi", "Nancy", "Pelosi", "D", "House", "CA", "11", ["Intelligence"]),
        "G000583": CongressMember("G000583", "Josh Gottheimer", "Josh", "Gottheimer", "D", "House", "NJ", "5", ["Financial Services", "Homeland Security"]),
        "F000472": CongressMember("F000472", "Scott Franklin", "Scott", "Franklin", "R", "House", "FL", "18", ["Appropriations", "AI Task Force"]),
        "C001123": CongressMember("C001123", "Gilbert Cisneros", "Gilbert", "Cisneros", "D", "House", "CA", "31", ["Armed Services", "Veterans' Affairs"]),
        "C001120": CongressMember("C001120", "Dan Crenshaw", "Dan", "Crenshaw", "R", "House", "TX", "2", ["Energy & Commerce", "Intelligence"]),
        "K000394": CongressMember("K000394", "Tom Kean Jr.", "Tom", "Kean", "R", "House", "NJ", "7", ["Foreign Affairs"]),
        "J000309": CongressMember("J000309", "Jonathan Jackson", "Jonathan", "Jackson", "D", "House", "IL", "1", ["Foreign Affairs", "Agriculture"]),
        "M001157": CongressMember("M001157", "Michael McCaul", "Michael", "McCaul", "R", "House", "TX", "10", ["Foreign Affairs", "Homeland Security"]),
        "K000389": CongressMember("K000389", "Ro Khanna", "Ro", "Khanna", "D", "House", "CA", "17", ["Armed Services", "Oversight"]),
        "F000246": CongressMember("F000246", "Pat Fallon", "Pat", "Fallon", "R", "House", "TX", "4", ["Armed Services", "Oversight"]),
        "D000617": CongressMember("D000617", "Suzan DelBene", "Suzan", "DelBene", "D", "House", "WA", "1", ["Ways & Means"]),
        "D000624": CongressMember("D000624", "Debbie Dingell", "Debbie", "Dingell", "D", "House", "MI", "6", ["Energy & Commerce"]),
        "M001163": CongressMember("M001163", "Doris Matsui", "Doris", "Matsui", "D", "House", "CA", "7", ["Energy & Commerce", "Rules"]),
        "S001156": CongressMember("S001156", "Linda Sánchez", "Linda", "Sánchez", "D", "House", "CA", "38", ["Ways & Means"]),
        "G000576": CongressMember("G000576", "Glenn Grothman", "Glenn", "Grothman", "R", "House", "WI", "6", ["Budget", "Oversight"]),
        
        # Senate Members - Top Traders
        "T000278": CongressMember("T000278", "Tommy Tuberville", "Tommy", "Tuberville", "R", "Senate", "AL", None, ["Armed Services", "Agriculture", "Veterans' Affairs"]),
        "M001190": CongressMember("M001190", "Markwayne Mullin", "Markwayne", "Mullin", "R", "Senate", "OK", None, ["Armed Services", "Environment", "Indian Affairs"]),
        "S001217": CongressMember("S001217", "Rick Scott", "Rick", "Scott", "R", "Senate", "FL", None, ["Armed Services", "Budget", "Commerce"]),
        "B001236": CongressMember("B001236", "John Boozman", "John", "Boozman", "R", "Senate", "AR", None, ["Agriculture", "Appropriations"]),
        "M000355": CongressMember("M000355", "Mitch McConnell", "Mitch", "McConnell", "R", "Senate", "KY", None, ["Appropriations", "Agriculture", "Rules"]),
        "H001042": CongressMember("H001042", "John Hickenlooper", "John", "Hickenlooper", "D", "Senate", "CO", None, ["Commerce", "Energy", "Small Business"]),
        "P000595": CongressMember("P000595", "Gary Peters", "Gary", "Peters", "D", "Senate", "MI", None, ["Armed Services", "Homeland Security"]),
        "W000779": CongressMember("W000779", "Ron Wyden", "Ron", "Wyden", "D", "Senate", "OR", None, ["Finance", "Budget", "Intelligence"]),
        "W000802": CongressMember("W000802", "Sheldon Whitehouse", "Sheldon", "Whitehouse", "D", "Senate", "RI", None, ["Judiciary", "Budget", "Environment"]),
        "M001242": CongressMember("M001242", "Bernie Moreno", "Bernie", "Moreno", "R", "Senate", "OH", None, ["Banking", "Commerce"]),
    }


class HouseDisclosureScraper:
    """
    Scraper for House Financial Disclosures.
    
    The House uses a web form interface at disclosures-clerk.house.gov
    """
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def _rate_limit(self):
        """Rate limit requests"""
        time.sleep(0.5)
    
    def search_member_filings(self, last_name: str, filing_year: int = None) -> List[Dict]:
        """
        Search for PTR filings by member last name.
        
        Args:
            last_name: Member's last name
            filing_year: Optional year filter
            
        Returns:
            List of filing metadata
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        # The House disclosure site uses a form POST
        search_url = f"{HOUSE_DISCLOSURES_BASE}/PublicDisclosure/FinancialDisclosure"
        
        try:
            self._rate_limit()
            
            # First get the search page to get any necessary tokens
            response = self.session.get(search_url)
            response.raise_for_status()
            
            # Parse for form tokens
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the search form and submit
            form_data = {
                "LastName": last_name,
                "FilingYear": str(filing_year),
                "State": "",
                "District": "",
                "ReportType": "T"  # T = Periodic Transaction Report
            }
            
            # Submit search
            self._rate_limit()
            search_response = self.session.post(
                f"{HOUSE_DISCLOSURES_BASE}/PublicDisclosure/FinancialDisclosure/ViewMemberSearchResult",
                data=form_data
            )
            search_response.raise_for_status()
            
            # Parse results
            return self._parse_house_search_results(search_response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error searching House filings for {last_name}: {e}")
            return []
    
    def _parse_house_search_results(self, html: str) -> List[Dict]:
        """Parse House disclosure search results HTML"""
        filings = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the results table
        table = soup.find('table', class_='library-table')
        if not table:
            return filings
        
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 5:
                # Extract link to PDF
                link = cols[0].find('a')
                pdf_url = link['href'] if link else None
                
                filings.append({
                    "name": cols[0].get_text(strip=True),
                    "office": cols[1].get_text(strip=True),
                    "filing_year": cols[2].get_text(strip=True),
                    "filing_type": cols[3].get_text(strip=True),
                    "filing_date": cols[4].get_text(strip=True),
                    "pdf_url": urljoin(HOUSE_DISCLOSURES_BASE, pdf_url) if pdf_url else None
                })
        
        return filings
    
    def get_ptr_transactions(self, pdf_url: str, member: CongressMember) -> List[StockTransaction]:
        """
        Download and parse a PTR PDF to extract transactions.
        
        Note: In production, you'd use a PDF parsing library like PyMuPDF or pdfplumber.
        For now, this returns a placeholder - real implementation would parse the PDF.
        """
        logger.info(f"Would parse PTR from: {pdf_url}")
        
        # In production, you would:
        # 1. Download the PDF
        # 2. Use pdfplumber or PyMuPDF to extract tables
        # 3. Parse the transaction data
        
        # Placeholder - returns empty list
        # Real implementation would parse the PDF
        return []
    
    def scrape_member_transactions(self, member: CongressMember, 
                                    filing_year: int = None) -> List[StockTransaction]:
        """
        Scrape all PTR transactions for a House member.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        transactions = []
        
        # Search for filings
        filings = self.search_member_filings(member.last_name, filing_year)
        
        # Filter to just this member (search might return multiple people)
        member_filings = [f for f in filings if member.last_name.lower() in f['name'].lower()]
        
        for filing in member_filings:
            if filing.get('pdf_url'):
                txns = self.get_ptr_transactions(filing['pdf_url'], member)
                transactions.extend(txns)
        
        return transactions
    
    def search_annual_disclosures(self, last_name: str, filing_year: int = None) -> List[Dict]:
        """
        Search for Annual Financial Disclosure (AFD) filings by member last name.
        
        AFDs contain complete asset/liability information for net worth calculation.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        search_url = f"{HOUSE_DISCLOSURES_BASE}/PublicDisclosure/FinancialDisclosure"
        
        try:
            self._rate_limit()
            response = self.session.get(search_url)
            response.raise_for_status()
            
            # Form data for Annual report search
            form_data = {
                "LastName": last_name,
                "FilingYear": str(filing_year),
                "State": "",
                "District": "",
                "ReportType": "A"  # A = Annual Financial Disclosure
            }
            
            self._rate_limit()
            search_response = self.session.post(
                f"{HOUSE_DISCLOSURES_BASE}/PublicDisclosure/FinancialDisclosure/ViewMemberSearchResult",
                data=form_data
            )
            search_response.raise_for_status()
            
            return self._parse_house_search_results(search_response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error searching House AFDs for {last_name}: {e}")
            return []
    
    def scrape_annual_disclosure(self, member: CongressMember, 
                                  filing_year: int = None) -> Optional[AnnualFinancialDisclosure]:
        """
        Scrape Annual Financial Disclosure for a House member.
        
        This extracts:
        - Schedule A: Assets
        - Schedule D: Liabilities
        - Spouse information
        - Income sources
        """
        if filing_year is None:
            filing_year = datetime.now().year - 1  # AFDs cover previous year
        
        # Search for annual disclosures
        filings = self.search_annual_disclosures(member.last_name, filing_year)
        
        # Filter to this member
        member_filings = [f for f in filings if member.last_name.lower() in f['name'].lower()]
        
        if not member_filings:
            logger.warning(f"No AFD found for {member.name} in {filing_year}")
            return None
        
        latest_filing = member_filings[0]
        
        if not latest_filing.get('pdf_url'):
            return None
        
        # Parse the AFD PDF
        return self._parse_afd_pdf(latest_filing['pdf_url'], member, filing_year)
    
    def _parse_afd_pdf(self, pdf_url: str, member: CongressMember, 
                       filing_year: int) -> Optional[AnnualFinancialDisclosure]:
        """
        Parse Annual Financial Disclosure PDF.
        
        Note: In production, use pdfplumber or PyMuPDF to extract tables.
        This implementation provides the structure for parsing.
        """
        logger.info(f"Would parse AFD from: {pdf_url}")
        
        # Create disclosure object with placeholder data
        # Real implementation would:
        # 1. Download PDF
        # 2. Use pdfplumber to extract Schedule A (Assets) table
        # 3. Extract Schedule D (Liabilities) table
        # 4. Parse spouse name from header
        # 5. Extract income information
        
        disclosure = AnnualFinancialDisclosure(
            member_id=member.bioguide_id,
            member_name=member.name,
            party=member.party,
            chamber=member.chamber,
            state=member.state,
            filing_year=filing_year,
            filing_date=datetime.now().strftime("%Y-%m-%d"),
            filing_url=pdf_url,
            assets=[],
            liabilities=[],
            income_sources=["Congressional Salary"]
        )
        
        disclosure.calculate_totals()
        return disclosure


class SenateDisclosureScraper:
    """
    Scraper for Senate Financial Disclosures.
    
    The Senate uses a search API at efdsearch.senate.gov
    """
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def _rate_limit(self):
        """Rate limit requests"""
        time.sleep(0.5)
    
    def _get_csrf_token(self) -> Optional[str]:
        """Get CSRF token from Senate disclosure site"""
        try:
            self._rate_limit()
            response = self.session.get(f"{SENATE_DISCLOSURES_BASE}/search/home/")
            response.raise_for_status()
            
            # Parse for CSRF token
            soup = BeautifulSoup(response.text, 'html.parser')
            csrf_input = soup.find('input', {'name': 'csrfmiddlewaretoken'})
            
            if csrf_input:
                return csrf_input['value']
            
            # Also check cookies
            return self.session.cookies.get('csrftoken')
            
        except requests.RequestException as e:
            logger.error(f"Error getting CSRF token: {e}")
            return None
    
    def search_senator_filings(self, first_name: str, last_name: str, 
                               filing_year: int = None) -> List[Dict]:
        """
        Search for PTR filings by senator name.
        
        Args:
            first_name: Senator's first name
            last_name: Senator's last name
            filing_year: Optional year filter
            
        Returns:
            List of filing metadata
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        # Get CSRF token first
        csrf_token = self._get_csrf_token()
        
        search_url = f"{SENATE_DISCLOSURES_BASE}/search/"
        
        # The Senate search uses JSON API
        search_data = {
            "first_name": first_name,
            "last_name": last_name,
            "filer_type": "1",  # Senator
            "report_type": "11",  # Periodic Transaction Report
            "submitted_start_date": f"01/01/{filing_year}",
            "submitted_end_date": f"12/31/{filing_year}",
        }
        
        headers = {
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "X-CSRFToken": csrf_token or "",
            "Referer": f"{SENATE_DISCLOSURES_BASE}/search/home/",
        }
        
        try:
            self._rate_limit()
            response = self.session.post(
                search_url,
                data=search_data,
                headers=headers
            )
            response.raise_for_status()
            
            return self._parse_senate_search_results(response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error searching Senate filings: {e}")
            return []
    
    def _parse_senate_search_results(self, html: str) -> List[Dict]:
        """Parse Senate disclosure search results"""
        filings = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find results in the table or JSON response
        table = soup.find('table', class_='table')
        if not table:
            return filings
        
        rows = table.find_all('tr')[1:]  # Skip header
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                link = cols[0].find('a')
                report_url = link['href'] if link else None
                
                filings.append({
                    "name": cols[0].get_text(strip=True),
                    "office": cols[1].get_text(strip=True) if len(cols) > 1 else "",
                    "report_type": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "filing_date": cols[3].get_text(strip=True) if len(cols) > 3 else "",
                    "report_url": urljoin(SENATE_DISCLOSURES_BASE, report_url) if report_url else None
                })
        
        return filings
    
    def get_ptr_details(self, report_url: str, member: CongressMember) -> List[StockTransaction]:
        """
        Fetch and parse PTR details from Senate disclosure site.
        
        The Senate provides transaction data in HTML tables on the report page.
        """
        try:
            self._rate_limit()
            response = self.session.get(report_url)
            response.raise_for_status()
            
            return self._parse_ptr_transactions(response.text, member, report_url)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching PTR details: {e}")
            return []
    
    def _parse_ptr_transactions(self, html: str, member: CongressMember, 
                                filing_url: str) -> List[StockTransaction]:
        """Parse transaction data from Senate PTR HTML"""
        transactions = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the transactions table
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            # Check if this is a transactions table
            if not any(h in ['asset', 'transaction', 'amount'] for h in headers):
                continue
            
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                try:
                    # Parse transaction
                    asset_name = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                    txn_type = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    txn_date = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                    amount = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                    
                    # Parse amount range
                    amount_min, amount_max = self._parse_amount_range(amount)
                    
                    # Try to extract ticker
                    ticker = self._extract_ticker(asset_name)
                    
                    transaction = StockTransaction(
                        member_id=member.bioguide_id,
                        member_name=member.name,
                        party=member.party,
                        chamber=member.chamber,
                        state=member.state,
                        transaction_date=txn_date,
                        filing_date=datetime.now().strftime("%Y-%m-%d"),  # Approximate
                        ticker=ticker,
                        asset_name=asset_name,
                        asset_type="Stock",  # Default, would need better parsing
                        transaction_type=self._normalize_txn_type(txn_type),
                        amount_range=amount,
                        amount_min=amount_min,
                        amount_max=amount_max,
                        owner="Self",  # Default
                        committees=member.committees,
                        filing_url=filing_url
                    )
                    transactions.append(transaction)
                    
                except Exception as e:
                    logger.warning(f"Error parsing transaction row: {e}")
                    continue
        
        return transactions
    
    def _parse_amount_range(self, amount_str: str) -> Tuple[int, int]:
        """Parse amount range string like '$1,001 - $15,000'"""
        # Remove $ and commas
        amount_str = amount_str.replace("$", "").replace(",", "")
        
        # Common ranges
        ranges = {
            "1001 - 15000": (1001, 15000),
            "15001 - 50000": (15001, 50000),
            "50001 - 100000": (50001, 100000),
            "100001 - 250000": (100001, 250000),
            "250001 - 500000": (250001, 500000),
            "500001 - 1000000": (500001, 1000000),
            "1000001 - 5000000": (1000001, 5000000),
            "5000001 - 25000000": (5000001, 25000000),
            "over 50000000": (50000000, 100000000),
        }
        
        # Try to match
        for pattern, values in ranges.items():
            if pattern in amount_str.lower().replace(" ", ""):
                return values
        
        # Try to extract numbers
        numbers = re.findall(r'\d+', amount_str)
        if len(numbers) >= 2:
            return int(numbers[0]), int(numbers[1])
        elif len(numbers) == 1:
            return int(numbers[0]), int(numbers[0])
        
        return 0, 0
    
    def _extract_ticker(self, asset_name: str) -> Optional[str]:
        """Extract stock ticker from asset name"""
        # Common patterns
        # "NVIDIA Corp (NVDA)" -> NVDA
        # "Apple Inc. - Common Stock" -> AAPL (from lookup)
        
        ticker_pattern = r'\(([A-Z]{1,5})\)'
        match = re.search(ticker_pattern, asset_name)
        if match:
            return match.group(1)
        
        # Known company to ticker mapping
        company_tickers = {
            "apple": "AAPL",
            "microsoft": "MSFT",
            "google": "GOOGL",
            "alphabet": "GOOGL",
            "amazon": "AMZN",
            "meta": "META",
            "facebook": "META",
            "nvidia": "NVDA",
            "tesla": "TSLA",
            "jpmorgan": "JPM",
            "berkshire": "BRK.B",
            "johnson & johnson": "JNJ",
            "procter": "PG",
            "visa": "V",
            "mastercard": "MA",
            "disney": "DIS",
            "netflix": "NFLX",
            "boeing": "BA",
            "lockheed": "LMT",
            "raytheon": "RTX",
            "northrop": "NOC",
            "general dynamics": "GD",
            "exxon": "XOM",
            "chevron": "CVX",
            "pfizer": "PFE",
            "merck": "MRK",
        }
        
        asset_lower = asset_name.lower()
        for company, ticker in company_tickers.items():
            if company in asset_lower:
                return ticker
        
        return None
    
    def _normalize_txn_type(self, txn_type: str) -> str:
        """Normalize transaction type string"""
        txn_lower = txn_type.lower()
        
        if "purchase" in txn_lower or "buy" in txn_lower:
            return "Purchase"
        elif "sale" in txn_lower and "partial" in txn_lower:
            return "Sale (Partial)"
        elif "sale" in txn_lower and "full" in txn_lower:
            return "Sale (Full)"
        elif "sale" in txn_lower or "sell" in txn_lower:
            return "Sale"
        elif "exchange" in txn_lower:
            return "Exchange"
        
        return txn_type
    
    def search_annual_disclosures(self, first_name: str, last_name: str,
                                   filing_year: int = None) -> List[Dict]:
        """
        Search for Annual Financial Disclosure (AFD) filings by senator name.
        
        Report type codes:
        - 11 = Periodic Transaction Report (PTR)
        - 7 = Annual Report
        - 6 = Amendment
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        csrf_token = self._get_csrf_token()
        search_url = f"{SENATE_DISCLOSURES_BASE}/search/"
        
        search_data = {
            "first_name": first_name,
            "last_name": last_name,
            "filer_type": "1",  # Senator
            "report_type": "7",  # Annual Report
            "submitted_start_date": f"01/01/{filing_year}",
            "submitted_end_date": f"12/31/{filing_year}",
        }
        
        headers = {
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "X-CSRFToken": csrf_token or "",
            "Referer": f"{SENATE_DISCLOSURES_BASE}/search/home/",
        }
        
        try:
            self._rate_limit()
            response = self.session.post(
                search_url,
                data=search_data,
                headers=headers
            )
            response.raise_for_status()
            
            return self._parse_senate_search_results(response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error searching Senate AFDs: {e}")
            return []
    
    def scrape_annual_disclosure(self, member: CongressMember,
                                  filing_year: int = None) -> Optional[AnnualFinancialDisclosure]:
        """
        Scrape Annual Financial Disclosure for a Senator.
        
        Senate AFDs are typically filed in May for the previous calendar year.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        filings = self.search_annual_disclosures(
            member.first_name, member.last_name, filing_year
        )
        
        if not filings:
            logger.warning(f"No AFD found for {member.name} in {filing_year}")
            return None
        
        latest_filing = filings[0]
        
        if not latest_filing.get('report_url'):
            return None
        
        return self._parse_afd_page(latest_filing['report_url'], member, filing_year)
    
    def _parse_afd_page(self, report_url: str, member: CongressMember,
                        filing_year: int) -> Optional[AnnualFinancialDisclosure]:
        """
        Parse Senate Annual Financial Disclosure page.
        
        Senate AFDs are displayed as HTML pages with multiple sections:
        - Part 1: Positions
        - Part 2: Earned and Non-Investment Income  
        - Part 3: Assets and Unearned Income
        - Part 4: Transactions (covered separately by PTRs)
        - Part 5: Liabilities
        - Part 6: Agreements
        - Part 7: Compensation
        - Part 8: Gifts
        - Part 9: Travel
        """
        try:
            self._rate_limit()
            response = self.session.get(report_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Initialize disclosure
            disclosure = AnnualFinancialDisclosure(
                member_id=member.bioguide_id,
                member_name=member.name,
                party=member.party,
                chamber=member.chamber,
                state=member.state,
                filing_year=filing_year,
                filing_date=datetime.now().strftime("%Y-%m-%d"),
                filing_url=report_url,
                assets=[],
                liabilities=[],
                income_sources=["Senate Salary"]
            )
            
            # Try to extract spouse name from header
            spouse_elem = soup.find(string=re.compile(r'spouse', re.IGNORECASE))
            if spouse_elem:
                # Parse spouse name from surrounding text
                parent_text = spouse_elem.parent.get_text() if spouse_elem.parent else ""
                spouse_match = re.search(r'Spouse:?\s*([A-Za-z\s]+)', parent_text)
                if spouse_match:
                    disclosure.spouse_name = spouse_match.group(1).strip()
            
            # Parse Part 3: Assets
            assets = self._parse_senate_assets(soup)
            disclosure.assets = assets
            
            # Parse Part 5: Liabilities
            liabilities = self._parse_senate_liabilities(soup)
            disclosure.liabilities = liabilities
            
            # Parse Part 2: Income sources
            income_sources = self._parse_senate_income(soup)
            disclosure.income_sources.extend(income_sources)
            
            # Calculate totals
            disclosure.calculate_totals()
            
            return disclosure
            
        except requests.RequestException as e:
            logger.error(f"Error fetching Senate AFD: {e}")
            return None
    
    def _parse_senate_assets(self, soup: BeautifulSoup) -> List[Asset]:
        """Parse Part 3: Assets and Unearned Income from Senate AFD"""
        assets = []
        
        # Find the assets table/section
        # Look for tables with asset-related headers
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            # Check if this looks like an assets table
            if not any(h in ['asset', 'value', 'income'] for h in headers):
                continue
            
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 3:
                    continue
                
                try:
                    description = cols[0].get_text(strip=True)
                    value_str = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                    income_str = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    
                    # Categorize asset
                    category = self._categorize_asset(description)
                    
                    # Parse value range
                    value_min, value_max = self._parse_amount_range(value_str)
                    
                    # Parse income if present
                    income_min, income_max = (0, 0)
                    if income_str:
                        income_min, income_max = self._parse_amount_range(income_str)
                    
                    if value_min > 0 or value_max > 0:
                        assets.append(Asset(
                            category=category,
                            description=description,
                            value_min=value_min,
                            value_max=value_max,
                            income_min=income_min if income_min > 0 else None,
                            income_max=income_max if income_max > 0 else None,
                        ))
                        
                except Exception as e:
                    logger.warning(f"Error parsing asset row: {e}")
                    continue
        
        return assets
    
    def _parse_senate_liabilities(self, soup: BeautifulSoup) -> List[Liability]:
        """Parse Part 5: Liabilities from Senate AFD"""
        liabilities = []
        
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            # Check if this looks like a liabilities table
            if not any(h in ['liability', 'creditor', 'amount'] for h in headers):
                continue
            
            rows = table.find_all('tr')[1:]
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                
                try:
                    creditor = cols[0].get_text(strip=True)
                    description = cols[1].get_text(strip=True) if len(cols) > 1 else creditor
                    amount_str = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    
                    value_min, value_max = self._parse_amount_range(amount_str)
                    
                    if value_min > 0 or value_max > 0:
                        liabilities.append(Liability(
                            description=description,
                            creditor=creditor,
                            value_min=value_min,
                            value_max=value_max,
                        ))
                        
                except Exception as e:
                    logger.warning(f"Error parsing liability row: {e}")
                    continue
        
        return liabilities
    
    def _parse_senate_income(self, soup: BeautifulSoup) -> List[str]:
        """Parse Part 2: Earned Income sources"""
        income_sources = []
        
        # Look for income-related sections
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            if not any(h in ['source', 'income', 'earned'] for h in headers):
                continue
            
            rows = table.find_all('tr')[1:]
            
            for row in rows:
                cols = row.find_all('td')
                if cols:
                    source = cols[0].get_text(strip=True)
                    if source and source not in income_sources:
                        income_sources.append(source)
        
        return income_sources
    
    def _categorize_asset(self, description: str) -> str:
        """Categorize asset based on description"""
        desc_lower = description.lower()
        
        # Real Estate
        if any(word in desc_lower for word in ['real estate', 'property', 'residence', 
                                                'home', 'land', 'house', 'condo', 'farm']):
            return "Real Estate"
        
        # Stocks
        if any(word in desc_lower for word in ['stock', 'common', 'share', 'equity',
                                                'corp', 'inc', 'ltd', 'llc']):
            return "Stocks"
        
        # Business Interest
        if any(word in desc_lower for word in ['business', 'partnership', 'llc member',
                                                'ownership', 'venture', 'capital']):
            return "Business Interest"
        
        # Retirement
        if any(word in desc_lower for word in ['401k', 'ira', 'pension', 'retirement',
                                                'tsp', 'thrift']):
            return "Retirement"
        
        # Cash/Bank
        if any(word in desc_lower for word in ['bank', 'cash', 'money market', 'checking',
                                                'savings', 'cd ', 'certificate']):
            return "Cash"
        
        # Bonds/Fixed Income
        if any(word in desc_lower for word in ['bond', 'treasury', 'municipal', 'note']):
            return "Bonds"
        
        # Mutual Funds
        if any(word in desc_lower for word in ['fund', 'mutual', 'etf', 'index']):
            return "Mutual Funds"
        
        return "Other"


class CongressionalTradingScraper:
    """
    Main scraper class that combines House and Senate disclosure scraping.
    """
    
    def __init__(self, data_dir: str = "./data/congress"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.house_scraper = HouseDisclosureScraper(data_dir)
        self.senate_scraper = SenateDisclosureScraper(data_dir)
    
    def scrape_member(self, member: CongressMember, 
                      filing_year: int = None) -> List[StockTransaction]:
        """
        Scrape transactions for a single Congress member.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        logger.info(f"Scraping transactions for {member.name}")
        
        if member.chamber == "House":
            return self.house_scraper.scrape_member_transactions(member, filing_year)
        else:
            # Senate
            filings = self.senate_scraper.search_senator_filings(
                member.first_name, member.last_name, filing_year
            )
            
            transactions = []
            for filing in filings:
                if filing.get('report_url'):
                    txns = self.senate_scraper.get_ptr_details(
                        filing['report_url'], member
                    )
                    transactions.extend(txns)
            
            return transactions
    
    def scrape_all_members(self, filing_year: int = None) -> Dict[str, List[StockTransaction]]:
        """
        Scrape transactions for all tracked Congress members.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        all_transactions = {}
        
        for member_id, member in CONGRESS_MEMBERS.items():
            try:
                transactions = self.scrape_member(member, filing_year)
                all_transactions[member_id] = transactions
                
                # Save individual member data
                self._save_member_transactions(member, transactions)
                
            except Exception as e:
                logger.error(f"Error scraping {member.name}: {e}")
                continue
        
        # Save combined data
        self._save_all_transactions(all_transactions)
        
        return all_transactions
    
    def _save_member_transactions(self, member: CongressMember, 
                                   transactions: List[StockTransaction]):
        """Save transactions for a single member"""
        filename = f"transactions_{member.bioguide_id}.json"
        filepath = self.data_dir / filename
        
        data = {
            "member": asdict(member),
            "last_updated": datetime.now().isoformat(),
            "transactions": [t.to_dict() for t in transactions]
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved {len(transactions)} transactions for {member.name}")
    
    def _save_all_transactions(self, all_transactions: Dict[str, List[StockTransaction]]):
        """Save all transactions to combined file"""
        filepath = self.data_dir / "all_congressional_trades.json"
        
        # Flatten all transactions into a single list, sorted by date
        all_txns = []
        for member_id, transactions in all_transactions.items():
            all_txns.extend(transactions)
        
        # Sort by transaction date (most recent first)
        all_txns.sort(key=lambda t: t.transaction_date, reverse=True)
        
        data = {
            "last_updated": datetime.now().isoformat(),
            "total_transactions": len(all_txns),
            "transactions": [t.to_dict() for t in all_txns]
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved {len(all_txns)} total transactions")
    
    def get_recent_trades(self, days: int = 30) -> List[StockTransaction]:
        """
        Get transactions from the last N days.
        """
        cutoff = datetime.now() - timedelta(days=days)
        all_transactions = self.scrape_all_members()
        
        recent = []
        for member_id, transactions in all_transactions.items():
            for txn in transactions:
                try:
                    txn_date = datetime.strptime(txn.transaction_date, "%m/%d/%Y")
                    if txn_date >= cutoff:
                        recent.append(txn)
                except ValueError:
                    continue
        
        return sorted(recent, key=lambda t: t.transaction_date, reverse=True)
    
    # ==========================================
    # NET WORTH / ANNUAL FINANCIAL DISCLOSURE
    # ==========================================
    
    def scrape_member_net_worth(self, member: CongressMember, 
                                 filing_year: int = None) -> Optional[AnnualFinancialDisclosure]:
        """
        Scrape Annual Financial Disclosure (net worth data) for a single member.
        
        AFDs are filed annually (typically in May) and contain:
        - Complete asset inventory with value ranges
        - Liabilities over $10,000
        - Earned income sources
        - Spouse financial information (if joint filer)
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        logger.info(f"Scraping net worth for {member.name}")
        
        if member.chamber == "House":
            return self.house_scraper.scrape_annual_disclosure(member, filing_year)
        else:
            return self.senate_scraper.scrape_annual_disclosure(member, filing_year)
    
    def scrape_all_net_worth(self, filing_year: int = None) -> Dict[str, AnnualFinancialDisclosure]:
        """
        Scrape net worth data for all tracked Congress members.
        """
        if filing_year is None:
            filing_year = datetime.now().year
        
        all_net_worth = {}
        
        for member_id, member in CONGRESS_MEMBERS.items():
            try:
                disclosure = self.scrape_member_net_worth(member, filing_year)
                if disclosure:
                    all_net_worth[member_id] = disclosure
                    
                    # Save individual member data
                    self._save_member_net_worth(member, disclosure)
                    
            except Exception as e:
                logger.error(f"Error scraping net worth for {member.name}: {e}")
                continue
        
        # Save combined data
        self._save_all_net_worth(all_net_worth)
        
        return all_net_worth
    
    def _save_member_net_worth(self, member: CongressMember, 
                                disclosure: AnnualFinancialDisclosure):
        """Save net worth data for a single member"""
        filename = f"networth_{member.bioguide_id}.json"
        filepath = self.data_dir / filename
        
        data = {
            "member": asdict(member),
            "last_updated": datetime.now().isoformat(),
            "disclosure": disclosure.to_dict()
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved net worth for {member.name}: ${disclosure.net_worth_min:,} - ${disclosure.net_worth_max:,}")
    
    def _save_all_net_worth(self, all_net_worth: Dict[str, AnnualFinancialDisclosure]):
        """Save all net worth data to combined file"""
        filepath = self.data_dir / "all_congressional_networth.json"
        
        # Create summary with rankings
        net_worth_list = []
        for member_id, disclosure in all_net_worth.items():
            net_worth_list.append({
                "member_id": member_id,
                "name": disclosure.member_name,
                "party": disclosure.party,
                "chamber": disclosure.chamber,
                "state": disclosure.state,
                "net_worth_min": disclosure.net_worth_min,
                "net_worth_max": disclosure.net_worth_max,
                "net_worth_midpoint": (disclosure.net_worth_min + disclosure.net_worth_max) // 2,
                "total_assets_min": disclosure.total_assets_min,
                "total_assets_max": disclosure.total_assets_max,
                "total_liabilities_min": disclosure.total_liabilities_min,
                "total_liabilities_max": disclosure.total_liabilities_max,
                "asset_count": len(disclosure.assets),
                "liability_count": len(disclosure.liabilities),
            })
        
        # Sort by net worth midpoint (descending)
        net_worth_list.sort(key=lambda x: x["net_worth_midpoint"], reverse=True)
        
        # Add rankings
        for i, item in enumerate(net_worth_list, 1):
            item["rank"] = i
        
        data = {
            "last_updated": datetime.now().isoformat(),
            "total_members": len(net_worth_list),
            "summary": net_worth_list,
            "disclosures": {mid: d.to_dict() for mid, d in all_net_worth.items()}
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved net worth for {len(net_worth_list)} members")
    
    def get_wealthiest_members(self, limit: int = 20) -> List[Dict]:
        """
        Get the wealthiest Congress members based on saved data.
        """
        filepath = self.data_dir / "all_congressional_networth.json"
        
        if not filepath.exists():
            logger.warning("No net worth data found. Run scrape_all_net_worth() first.")
            return []
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return data.get("summary", [])[:limit]


def main():
    """Run the congressional trading scraper"""
    scraper = CongressionalTradingScraper(data_dir="./data/congress")
    
    # Example: Scrape a single senator
    tuberville = CONGRESS_MEMBERS["T000278"]
    print(f"\nSearching for {tuberville.name} filings...")
    
    filings = scraper.senate_scraper.search_senator_filings(
        tuberville.first_name, 
        tuberville.last_name,
        2024
    )
    print(f"Found {len(filings)} filings")
    for f in filings[:5]:
        print(f"  - {f['filing_date']}: {f['report_type']}")
    
    # Example: Scrape a House member
    pelosi = CONGRESS_MEMBERS["P000197"]
    print(f"\nSearching for {pelosi.name} filings...")
    
    filings = scraper.house_scraper.search_member_filings(pelosi.last_name, 2024)
    print(f"Found {len(filings)} filings")
    for f in filings[:5]:
        print(f"  - {f['filing_date']}: {f.get('filing_type', 'N/A')}")
    
    # Example: Scrape net worth
    # print(f"\nScraping net worth for {pelosi.name}...")
    # net_worth = scraper.scrape_member_net_worth(pelosi, 2024)
    # if net_worth:
    #     print(f"Net worth: ${net_worth.net_worth_min:,} - ${net_worth.net_worth_max:,}")
    
    # Uncomment to scrape all members
    # all_transactions = scraper.scrape_all_members(2024)
    # print(f"\nScraped transactions for {len(all_transactions)} members")
    
    # Uncomment to scrape all net worth data
    # all_net_worth = scraper.scrape_all_net_worth(2024)
    # print(f"\nScraped net worth for {len(all_net_worth)} members")


if __name__ == "__main__":
    main()
