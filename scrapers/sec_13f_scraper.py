"""
SEC 13F Filing Scraper for InvestorInsight
Scrapes 13F-HR filings from SEC EDGAR to track superinvestor holdings.

13F filings are required quarterly from institutional investment managers
with >$100M AUM. Filed within 45 days of quarter end.
"""

import requests
import json
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SEC EDGAR API endpoints
SEC_EDGAR_BASE = "https://www.sec.gov"
SEC_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
SEC_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"
SEC_EDGAR_ARCHIVES = "https://www.sec.gov/cgi-bin/browse-edgar"

# Required headers for SEC EDGAR (they require user-agent identification)
HEADERS = {
    "User-Agent": "InvestorInsight Research Bot (contact@investorinsight.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, application/xml"
}

# List of 81 superinvestors to track (CIK numbers)
# CIK = Central Index Key - SEC's unique identifier for filers
SUPERINVESTORS = {
    # Warren Buffett - Berkshire Hathaway
    "1067983": {
        "name": "Warren Buffett",
        "firm": "Berkshire Hathaway Inc",
        "cik": "1067983"
    },
    # David Einhorn - Greenlight Capital
    "1079114": {
        "name": "David Einhorn",
        "firm": "Greenlight Capital Inc",
        "cik": "1079114"
    },
    # Bill Ackman - Pershing Square
    "1336528": {
        "name": "Bill Ackman",
        "firm": "Pershing Square Capital Management",
        "cik": "1336528"
    },
    # Seth Klarman - Baupost Group
    "1061768": {
        "name": "Seth Klarman",
        "firm": "Baupost Group LLC",
        "cik": "1061768"
    },
    # Carl Icahn - Icahn Enterprises
    "921669": {
        "name": "Carl Icahn",
        "firm": "Icahn Capital LP",
        "cik": "921669"
    },
    # Michael Burry - Scion Asset Management
    "1649339": {
        "name": "Michael Burry",
        "firm": "Scion Asset Management LLC",
        "cik": "1649339"
    },
    # Howard Marks - Oaktree Capital
    "949509": {
        "name": "Howard Marks",
        "firm": "Oaktree Capital Management LP",
        "cik": "949509"
    },
    # Joel Greenblatt - Gotham Asset Management
    "1510387": {
        "name": "Joel Greenblatt",
        "firm": "Gotham Asset Management LLC",
        "cik": "1510387"
    },
    # Mohnish Pabrai - Pabrai Investment Funds
    "1173334": {
        "name": "Mohnish Pabrai",
        "firm": "Pabrai Investment Funds",
        "cik": "1173334"
    },
    # Ray Dalio - Bridgewater Associates
    "1350694": {
        "name": "Ray Dalio",
        "firm": "Bridgewater Associates LP",
        "cik": "1350694"
    },
    # David Tepper - Appaloosa Management
    "1656456": {
        "name": "David Tepper",
        "firm": "Appaloosa Management LP",
        "cik": "1656456"
    },
    # Chase Coleman - Tiger Global
    "1167483": {
        "name": "Chase Coleman",
        "firm": "Tiger Global Management LLC",
        "cik": "1167483"
    },
    # Stanley Druckenmiller - Duquesne Family Office
    "1536411": {
        "name": "Stanley Druckenmiller",
        "firm": "Duquesne Family Office LLC",
        "cik": "1536411"
    },
    # Dan Loeb - Third Point
    "1040273": {
        "name": "Dan Loeb",
        "firm": "Third Point LLC",
        "cik": "1040273"
    },
    # Nelson Peltz - Trian Fund Management
    "1345471": {
        "name": "Nelson Peltz",
        "firm": "Trian Fund Management LP",
        "cik": "1345471"
    },
    # Paul Singer - Elliott Management
    "1048445": {
        "name": "Paul Singer",
        "firm": "Elliott Investment Management LP",
        "cik": "1048445"
    },
    # Leon Cooperman - Omega Advisors
    "1657335": {
        "name": "Leon Cooperman",
        "firm": "Omega Advisors Inc",
        "cik": "1657335"
    },
    # Chris Hohn - TCI Fund Management
    "1647251": {
        "name": "Chris Hohn",
        "firm": "TCI Fund Management Ltd",
        "cik": "1647251"
    },
    # Jeffrey Ubben - Inclusive Capital
    "1802994": {
        "name": "Jeffrey Ubben",
        "firm": "Inclusive Capital Partners LP",
        "cik": "1802994"
    },
    # Terry Smith - Fundsmith
    "1569205": {
        "name": "Terry Smith",
        "firm": "Fundsmith LLP",
        "cik": "1569205"
    },
    # Li Lu - Himalaya Capital
    "1709323": {
        "name": "Li Lu",
        "firm": "Himalaya Capital Management LLC",
        "cik": "1709323"
    },
    # Guy Spier - Aquamarine Capital
    "1549341": {
        "name": "Guy Spier",
        "firm": "Aquamarine Capital Management LLC",
        "cik": "1549341"
    },
    # Tom Gayner - Markel
    "1096343": {
        "name": "Tom Gayner",
        "firm": "Markel Corporation",
        "cik": "1096343"
    },
    # Chuck Akre - Akre Capital Management
    "1112520": {
        "name": "Chuck Akre",
        "firm": "Akre Capital Management LLC",
        "cik": "1112520"
    },
    # Pat Dorsey - Dorsey Asset Management
    "1766596": {
        "name": "Pat Dorsey",
        "firm": "Dorsey Asset Management LLC",
        "cik": "1766596"
    },
}


@dataclass
class Holding:
    """Represents a single stock holding from a 13F filing"""
    cusip: str
    issuer_name: str
    class_title: str
    value: int  # in thousands of dollars
    shares: int
    share_type: str  # SH (shares) or PRN (principal amount)
    investment_discretion: str
    voting_authority_sole: int
    voting_authority_shared: int
    voting_authority_none: int
    
    # Computed fields
    ticker: Optional[str] = None
    pct_portfolio: Optional[float] = None


@dataclass
class Filing13F:
    """Represents a complete 13F-HR filing"""
    cik: str
    accession_number: str
    filing_date: str
    report_date: str  # Quarter end date
    investor_name: str
    firm_name: str
    total_value: int  # in thousands
    holdings: List[Holding]
    
    def to_dict(self):
        return {
            **asdict(self),
            'holdings': [asdict(h) for h in self.holdings]
        }


class SEC13FScraper:
    """
    Scraper for SEC EDGAR 13F-HR filings.
    
    13F-HR filings contain quarterly holdings data for institutional
    investment managers with >$100M AUM.
    """
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cusip_to_ticker = self._load_cusip_mapping()
        
    def _load_cusip_mapping(self) -> Dict[str, str]:
        """
        Load CUSIP to ticker symbol mapping.
        In production, this would be loaded from a database or API.
        """
        # Common CUSIP mappings (first 6 digits of CUSIP)
        return {
            "037833": "AAPL",   # Apple
            "02079K": "GOOGL",  # Alphabet Class A
            "02079L": "GOOG",   # Alphabet Class C
            "594918": "MSFT",   # Microsoft
            "023135": "AMZN",   # Amazon
            "30303M": "META",   # Meta Platforms
            "67066G": "NVDA",   # NVIDIA
            "88160R": "TSLA",   # Tesla
            "084670": "BRK.B",  # Berkshire Hathaway
            "060505": "BAC",    # Bank of America
            "46625H": "JPM",    # JPMorgan
            "92826C": "V",      # Visa
            "478160": "JNJ",    # Johnson & Johnson
            "931142": "WMT",    # Walmart
            "742718": "PG",     # Procter & Gamble
            "88579Y": "MA",     # Mastercard
            "172967": "C",      # Citigroup
            "254687": "DIS",    # Disney
            "459200": "IBM",    # IBM
            "713448": "PEP",    # PepsiCo
            "191216": "KO",     # Coca-Cola
            "166764": "CVX",    # Chevron
            "30231G": "XOM",    # Exxon Mobil
        }
    
    def _rate_limit(self):
        """SEC EDGAR requires max 10 requests per second"""
        time.sleep(0.15)
    
    def get_cik_filings(self, cik: str, filing_type: str = "13F-HR") -> List[Dict]:
        """
        Get list of filings for a given CIK.
        
        Args:
            cik: SEC Central Index Key
            filing_type: Type of filing (default 13F-HR)
            
        Returns:
            List of filing metadata dictionaries
        """
        # Pad CIK to 10 digits
        cik_padded = cik.zfill(10)
        
        url = f"{SEC_EDGAR_SUBMISSIONS}/CIK{cik_padded}.json"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            
            filings = []
            recent_filings = data.get("filings", {}).get("recent", {})
            
            if not recent_filings:
                return []
            
            forms = recent_filings.get("form", [])
            accession_numbers = recent_filings.get("accessionNumber", [])
            filing_dates = recent_filings.get("filingDate", [])
            primary_documents = recent_filings.get("primaryDocument", [])
            
            for i, form in enumerate(forms):
                if form == filing_type or form == f"{filing_type}/A":  # Include amendments
                    filings.append({
                        "form": form,
                        "accession_number": accession_numbers[i],
                        "filing_date": filing_dates[i],
                        "primary_document": primary_documents[i] if i < len(primary_documents) else None
                    })
            
            return filings[:8]  # Last 8 filings (2 years of quarterly data)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching filings for CIK {cik}: {e}")
            return []
    
    def get_13f_holdings(self, cik: str, accession_number: str) -> Optional[List[Holding]]:
        """
        Parse 13F-HR information table to extract holdings.
        
        The holdings are in an XML file within the filing.
        """
        # Format accession number for URL
        accession_formatted = accession_number.replace("-", "")
        cik_padded = cik.zfill(10)
        
        # Try to find the information table XML file
        index_url = f"{SEC_EDGAR_BASE}/Archives/edgar/data/{cik}/{accession_formatted}/"
        
        try:
            self._rate_limit()
            response = requests.get(index_url, headers=HEADERS)
            response.raise_for_status()
            
            # Find the infotable XML file
            xml_pattern = r'href="([^"]*infotable[^"]*\.xml)"'
            matches = re.findall(xml_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                # Try alternative pattern
                xml_pattern = r'href="([^"]*form13f[^"]*\.xml)"'
                matches = re.findall(xml_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                # Try any XML that's not primary_doc
                xml_pattern = r'href="([^"]+\.xml)"'
                all_xml = re.findall(xml_pattern, response.text, re.IGNORECASE)
                matches = [x for x in all_xml if 'primary_doc' not in x.lower()]
            
           if not matches:
                # Try any XML that's not primary_doc
                xml_pattern = r'href="([^"]+\.xml)"'
                all_xml = re.findall(xml_pattern, response.text, re.IGNORECASE)
                matches = [x for x in all_xml if 'primary_doc' not in x.lower()] 
               
        if not matches:
                logger.warning(f"No info table found for {accession_number}")
                return None
            
            xml_file = matches[0]
            xml_url = f"{index_url}{xml_file}"
            
            self._rate_limit()
            xml_response = requests.get(xml_url, headers=HEADERS)
            xml_response.raise_for_status()
            
            return self._parse_13f_xml(xml_response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching 13F holdings: {e}")
            return None
    
    def _parse_13f_xml(self, xml_content: str) -> List[Holding]:
        """Parse 13F information table XML"""
        holdings = []
        
        try:
            # Handle namespaces in SEC XML
            xml_content = re.sub(r'xmlns[^"]*"[^"]*"', '', xml_content)
            root = ET.fromstring(xml_content)
            
            # Find all infoTable entries
            for info_table in root.iter():
                if 'infotable' in info_table.tag.lower():
                    holding = self._parse_info_table_entry(info_table)
                    if holding:
                        holdings.append(holding)
                        
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            
        return holdings
    
    def _parse_info_table_entry(self, entry: ET.Element) -> Optional[Holding]:
        """Parse a single infoTable entry from 13F XML"""
        def get_text(elem: ET.Element, *tags: str) -> str:
            for tag in tags:
                for child in elem.iter():
                    if tag.lower() in child.tag.lower():
                        return child.text or ""
            return ""
        
        def get_int(elem: ET.Element, *tags: str) -> int:
            text = get_text(elem, *tags)
            try:
                return int(text.replace(",", ""))
            except ValueError:
                return 0
        
        cusip = get_text(entry, "cusip")
        if not cusip:
            return None
            
        # Get ticker from CUSIP mapping
        ticker = self.cusip_to_ticker.get(cusip[:6])
        
        return Holding(
            cusip=cusip,
            issuer_name=get_text(entry, "nameofissuer", "issuer"),
            class_title=get_text(entry, "titleofclass", "class"),
            value=get_int(entry, "value"),
            shares=get_int(entry, "sshprnamt", "shares"),
            share_type=get_text(entry, "sshprnamttype", "type") or "SH",
            investment_discretion=get_text(entry, "investmentdiscretion", "discretion") or "SOLE",
            voting_authority_sole=get_int(entry, "sole"),
            voting_authority_shared=get_int(entry, "shared"),
            voting_authority_none=get_int(entry, "none"),
            ticker=ticker
        )
    
    def scrape_investor(self, cik: str, investor_info: Dict) -> Optional[Filing13F]:
        """
        Scrape the most recent 13F filing for an investor.
        
        Args:
            cik: SEC Central Index Key
            investor_info: Dictionary with investor name and firm
            
        Returns:
            Filing13F object with holdings data
        """
        logger.info(f"Scraping 13F for {investor_info['name']} (CIK: {cik})")
        
        # Get list of filings
        filings = self.get_cik_filings(cik, "13F-HR")
        
        if not filings:
            logger.warning(f"No 13F filings found for CIK {cik}")
            return None
        
        # Get most recent filing
        latest = filings[0]
        
        # Get holdings
        holdings = self.get_13f_holdings(cik, latest["accession_number"])
        
        if not holdings:
            logger.warning(f"No holdings parsed for {latest['accession_number']}")
            return None
        
        # Calculate total value and portfolio percentages
        total_value = sum(h.value for h in holdings)
        for holding in holdings:
            if total_value > 0:
                holding.pct_portfolio = round((holding.value / total_value) * 100, 2)
        
        # Sort by value descending
        holdings.sort(key=lambda h: h.value, reverse=True)
        
        return Filing13F(
            cik=cik,
            accession_number=latest["accession_number"],
            filing_date=latest["filing_date"],
            report_date=self._get_quarter_end(latest["filing_date"]),
            investor_name=investor_info["name"],
            firm_name=investor_info["firm"],
            total_value=total_value,
            holdings=holdings
        )
    
    def _get_quarter_end(self, filing_date: str) -> str:
        """Calculate quarter end date from filing date (filed within 45 days)"""
        try:
            date = datetime.strptime(filing_date, "%Y-%m-%d")
            # Go back ~45 days to get approximate quarter end
            quarter_end = date - timedelta(days=45)
            # Round to quarter end
            month = quarter_end.month
            year = quarter_end.year
            if month <= 3:
                return f"{year}-03-31"
            elif month <= 6:
                return f"{year}-06-30"
            elif month <= 9:
                return f"{year}-09-30"
            else:
                return f"{year}-12-31"
        except ValueError:
            return filing_date
    
    def scrape_all_superinvestors(self) -> Dict[str, Filing13F]:
        """
        Scrape 13F filings for all tracked superinvestors.
        
        Returns:
            Dictionary mapping CIK to Filing13F objects
        """
        results = {}
        
        for cik, info in SUPERINVESTORS.items():
            try:
                filing = self.scrape_investor(cik, info)
                if filing:
                    results[cik] = filing
                    
                    # Save individual filing
                    self._save_filing(filing)
                    
            except Exception as e:
                logger.error(f"Error scraping {info['name']}: {e}")
                continue
        
        # Save combined results
        self._save_all_filings(results)
        
        return results
    
    def _save_filing(self, filing: Filing13F):
        """Save individual filing to JSON file"""
        filename = f"13f_{filing.cik}_{filing.filing_date}.json"
        filepath = self.data_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(filing.to_dict(), f, indent=2)
        
        logger.info(f"Saved filing to {filepath}")
    
    def _save_all_filings(self, filings: Dict[str, Filing13F]):
        """Save all filings to combined JSON file"""
        filepath = self.data_dir / "superinvestor_holdings.json"
        
        data = {
            "last_updated": datetime.now().isoformat(),
            "filings": {cik: f.to_dict() for cik, f in filings.items()}
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved all filings to {filepath}")
    
    def calculate_changes(self, current: Filing13F, previous: Filing13F) -> List[Dict]:
        """
        Calculate changes between two quarterly filings.
        
        Returns list of changes: added, increased, decreased, sold
        """
        changes = []
        
        current_holdings = {h.cusip: h for h in current.holdings}
        previous_holdings = {h.cusip: h for h in previous.holdings}
        
        # Check for new, increased, decreased positions
        for cusip, holding in current_holdings.items():
            if cusip not in previous_holdings:
                changes.append({
                    "cusip": cusip,
                    "ticker": holding.ticker,
                    "issuer": holding.issuer_name,
                    "change_type": "added",
                    "current_shares": holding.shares,
                    "current_value": holding.value,
                    "pct_change": 100.0
                })
            else:
                prev = previous_holdings[cusip]
                if holding.shares != prev.shares:
                    pct_change = ((holding.shares - prev.shares) / prev.shares) * 100
                    change_type = "increased" if pct_change > 0 else "decreased"
                    changes.append({
                        "cusip": cusip,
                        "ticker": holding.ticker,
                        "issuer": holding.issuer_name,
                        "change_type": change_type,
                        "current_shares": holding.shares,
                        "previous_shares": prev.shares,
                        "current_value": holding.value,
                        "pct_change": round(pct_change, 2)
                    })
        
        # Check for sold positions
        for cusip, holding in previous_holdings.items():
            if cusip not in current_holdings:
                changes.append({
                    "cusip": cusip,
                    "ticker": holding.ticker,
                    "issuer": holding.issuer_name,
                    "change_type": "sold",
                    "previous_shares": holding.shares,
                    "previous_value": holding.value,
                    "pct_change": -100.0
                })
        
        return changes


def main():
    """Run the 13F scraper"""
    scraper = SEC13FScraper(data_dir="./data/13f")
    
    # Example: Scrape single investor
    buffett = scraper.scrape_investor("1067983", SUPERINVESTORS["1067983"])
    if buffett:
        print(f"\n{buffett.investor_name} - {buffett.firm_name}")
        print(f"Filing Date: {buffett.filing_date}")
        print(f"Total Value: ${buffett.total_value:,}K")
        print(f"\nTop 10 Holdings:")
        for h in buffett.holdings[:10]:
            ticker = h.ticker or h.cusip[:6]
            print(f"  {ticker}: ${h.value:,}K ({h.pct_portfolio}%)")
    
    # Uncomment to scrape all superinvestors
    # results = scraper.scrape_all_superinvestors()
    # print(f"\nScraped {len(results)} investors")


if __name__ == "__main__":
    main()
