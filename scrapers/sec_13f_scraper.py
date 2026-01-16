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

# List of superinvestors to track (CIK numbers)
SUPERINVESTORS = {
    "1067983": {
        "name": "Warren Buffett",
        "firm": "Berkshire Hathaway Inc",
        "cik": "1067983"
    },
    "1079114": {
        "name": "David Einhorn",
        "firm": "Greenlight Capital Inc",
        "cik": "1079114"
    },
    "1336528": {
        "name": "Bill Ackman",
        "firm": "Pershing Square Capital Management",
        "cik": "1336528"
    },
    "1061768": {
        "name": "Seth Klarman",
        "firm": "Baupost Group LLC",
        "cik": "1061768"
    },
    "921669": {
        "name": "Carl Icahn",
        "firm": "Icahn Capital LP",
        "cik": "921669"
    },
    "1649339": {
        "name": "Michael Burry",
        "firm": "Scion Asset Management LLC",
        "cik": "1649339"
    },
    "949509": {
        "name": "Howard Marks",
        "firm": "Oaktree Capital Management LP",
        "cik": "949509"
    },
    "1510387": {
        "name": "Joel Greenblatt",
        "firm": "Gotham Asset Management LLC",
        "cik": "1510387"
    },
    "1173334": {
        "name": "Mohnish Pabrai",
        "firm": "Pabrai Investment Funds",
        "cik": "1173334"
    },
    "1350694": {
        "name": "Ray Dalio",
        "firm": "Bridgewater Associates LP",
        "cik": "1350694"
    },
    "1656456": {
        "name": "David Tepper",
        "firm": "Appaloosa Management LP",
        "cik": "1656456"
    },
    "1167483": {
        "name": "Chase Coleman",
        "firm": "Tiger Global Management LLC",
        "cik": "1167483"
    },
    "1536411": {
        "name": "Stanley Druckenmiller",
        "firm": "Duquesne Family Office LLC",
        "cik": "1536411"
    },
    "1040273": {
        "name": "Dan Loeb",
        "firm": "Third Point LLC",
        "cik": "1040273"
    },
    "1345471": {
        "name": "Nelson Peltz",
        "firm": "Trian Fund Management LP",
        "cik": "1345471"
    },
    "1048445": {
        "name": "Paul Singer",
        "firm": "Elliott Investment Management LP",
        "cik": "1048445"
    },
    "1657335": {
        "name": "Leon Cooperman",
        "firm": "Omega Advisors Inc",
        "cik": "1657335"
    },
    "1647251": {
        "name": "Chris Hohn",
        "firm": "TCI Fund Management Ltd",
        "cik": "1647251"
    },
    "1802994": {
        "name": "Jeffrey Ubben",
        "firm": "Inclusive Capital Partners LP",
        "cik": "1802994"
    },
    "1569205": {
        "name": "Terry Smith",
        "firm": "Fundsmith LLP",
        "cik": "1569205"
    },
    "1709323": {
        "name": "Li Lu",
        "firm": "Himalaya Capital Management LLC",
        "cik": "1709323"
    },
    "1549341": {
        "name": "Guy Spier",
        "firm": "Aquamarine Capital Management LLC",
        "cik": "1549341"
    },
    "1096343": {
        "name": "Tom Gayner",
        "firm": "Markel Corporation",
        "cik": "1096343"
    },
    "1112520": {
        "name": "Chuck Akre",
        "firm": "Akre Capital Management LLC",
        "cik": "1112520"
    },
    "1766596": {
        "name": "Pat Dorsey",
        "firm": "Dorsey Asset Management LLC",
        "cik": "1766596"
    },
}


@dataclass
class Holding:
    cusip: str
    issuer_name: str
    class_title: str
    value: int
    shares: int
    share_type: str
    investment_discretion: str
    voting_authority_sole: int
    voting_authority_shared: int
    voting_authority_none: int
    ticker: Optional[str] = None
    pct_portfolio: Optional[float] = None


@dataclass
class Filing13F:
    cik: str
    accession_number: str
    filing_date: str
    report_date: str
    investor_name: str
    firm_name: str
    total_value: int
    holdings: List[Holding]
    
    def to_dict(self):
        return {
            **asdict(self),
            'holdings': [asdict(h) for h in self.holdings]
        }


class SEC13FScraper:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cusip_to_ticker = self._load_cusip_mapping()
        
    def _load_cusip_mapping(self) -> Dict[str, str]:
        return {
            "037833": "AAPL",
            "02079K": "GOOGL",
            "02079L": "GOOG",
            "594918": "MSFT",
            "023135": "AMZN",
            "30303M": "META",
            "67066G": "NVDA",
            "88160R": "TSLA",
            "084670": "BRK.B",
            "060505": "BAC",
            "46625H": "JPM",
            "92826C": "V",
            "478160": "JNJ",
            "931142": "WMT",
            "742718": "PG",
            "88579Y": "MA",
            "172967": "C",
            "254687": "DIS",
            "459200": "IBM",
            "713448": "PEP",
            "191216": "KO",
            "166764": "CVX",
            "30231G": "XOM",
        }
    
    def _rate_limit(self):
        time.sleep(0.15)
    
    def get_cik_filings(self, cik: str, filing_type: str = "13F-HR") -> List[Dict]:
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
                if form == filing_type or form == f"{filing_type}/A":
                    filings.append({
                        "form": form,
                        "accession_number": accession_numbers[i],
                        "filing_date": filing_dates[i],
                        "primary_document": primary_documents[i] if i < len(primary_documents) else None
                    })
            
            return filings[:8]
            
        except requests.RequestException as e:
            logger.error(f"Error fetching filings for CIK {cik}: {e}")
            return []
    
    def get_13f_holdings(self, cik: str, accession_number: str) -> Optional[List[Holding]]:
        accession_formatted = accession_number.replace("-", "")
        cik_padded = cik.zfill(10)
        index_url = f"{SEC_EDGAR_BASE}/Archives/edgar/data/{cik}/{accession_formatted}/"
        
        try:
            self._rate_limit()
            response = requests.get(index_url, headers=HEADERS)
            response.raise_for_status()
            
            xml_pattern = r'href="([^"]*infotable[^"]*\.xml)"'
            matches = re.findall(xml_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                xml_pattern = r'href="([^"]*form13f[^"]*\.xml)"'
                matches = re.findall(xml_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                xml_pattern = r'href="([^"]+\.xml)"'
                all_xml = re.findall(xml_pattern, response.text, re.IGNORECASE)
                matches = [x for x in all_xml if 'primary_doc' not in x.lower()]
            
            if not matches:
                logger.warning(f"No info table found for {accession_number}")
                return None
            
            xml_file = matches[0]
            if xml_file.startswith('/'):
                xml_url = f"{SEC_EDGAR_BASE}{xml_file}"
            else:
                xml_url = f"{index_url}{xml_file}"
            
            self._rate_limit()
            xml_response = requests.get(xml_url, headers=HEADERS)
            xml_response.raise_for_status()
            
            return self._parse_13f_xml(xml_response.text)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching 13F holdings: {e}")
            return None
    
    def _parse_13f_xml(self, xml_content: str) -> List[Holding]:
        holdings = []
        
        info_tables = re.findall(r'<infoTable>(.*?)</infoTable>', xml_content, re.DOTALL)
        
        for table in info_tables:
            cusip_match = re.search(r'<cusip>([^<]+)</cusip>', table)
            if not cusip_match:
                continue
                
            cusip = cusip_match.group(1)
            
            name_match = re.search(r'<nameOfIssuer>([^<]+)</nameOfIssuer>', table)
            title_match = re.search(r'<titleOfClass>([^<]+)</titleOfClass>', table)
            value_match = re.search(r'<value>([^<]+)</value>', table)
            shares_match = re.search(r'<sshPrnamt>([^<]+)</sshPrnamt>', table)
            share_type_match = re.search(r'<sshPrnamtType>([^<]+)</sshPrnamtType>', table)
            discretion_match = re.search(r'<investmentDiscretion>([^<]+)</investmentDiscretion>', table)
            sole_match = re.search(r'<Sole>([^<]+)</Sole>', table)
            shared_match = re.search(r'<Shared>([^<]+)</Shared>', table)
            none_match = re.search(r'<None>([^<]+)</None>', table)
            
            ticker = self.cusip_to_ticker.get(cusip[:6])
            
            holdings.append(Holding(
                cusip=cusip,
                issuer_name=name_match.group(1) if name_match else "",
                class_title=title_match.group(1) if title_match else "",
                value=int(value_match.group(1)) if value_match else 0,
                shares=int(shares_match.group(1)) if shares_match else 0,
                share_type=share_type_match.group(1) if share_type_match else "SH",
                investment_discretion=discretion_match.group(1) if discretion_match else "SOLE",
                voting_authority_sole=int(sole_match.group(1)) if sole_match else 0,
                voting_authority_shared=int(shared_match.group(1)) if shared_match else 0,
                voting_authority_none=int(none_match.group(1)) if none_match else 0,
                ticker=ticker
            ))
        
        return holdings
    
    def scrape_investor(self, cik: str, investor_info: Dict) -> Optional[Filing13F]:
        logger.info(f"Scraping 13F for {investor_info['name']} (CIK: {cik})")
        
        filings = self.get_cik_filings(cik, "13F-HR")
        
        if not filings:
            logger.warning(f"No 13F filings found for CIK {cik}")
            return None
        
        latest = filings[0]
        holdings = self.get_13f_holdings(cik, latest["accession_number"])
        
        if not holdings:
            logger.warning(f"No holdings parsed for {latest['accession_number']}")
            return None
        
        total_value = sum(h.value for h in holdings)
        for holding in holdings:
            if total_value > 0:
                holding.pct_portfolio = round((holding.value / total_value) * 100, 2)
        
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
        try:
            date = datetime.strptime(filing_date, "%Y-%m-%d")
            quarter_end = date - timedelta(days=45)
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
        results = {}
        
        for cik, info in SUPERINVESTORS.items():
            try:
                filing = self.scrape_investor(cik, info)
                if filing:
                    results[cik] = filing
                    self._save_filing(filing)
            except Exception as e:
                logger.error(f"Error scraping {info['name']}: {e}")
                continue
        
        self._save_all_filings(results)
        return results
    
    def _save_filing(self, filing: Filing13F):
        filename = f"13f_{filing.cik}_{filing.filing_date}.json"
        filepath = self.data_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(filing.to_dict(), f, indent=2)
        
        logger.info(f"Saved filing to {filepath}")
    
    def _save_all_filings(self, filings: Dict[str, Filing13F]):
        filepath = self.data_dir / "superinvestor_holdings.json"
        
        data = {
            "last_updated": datetime.now().isoformat(),
            "filings": {cik: f.to_dict() for cik, f in filings.items()}
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved all filings to {filepath}")


def main():
    scraper = SEC13FScraper(data_dir="./data/13f")
    burry = scraper.scrape_investor("1649339", SUPERINVESTORS["1649339"])
    if burry:
        print(f"\n{burry.investor_name} - {burry.firm_name}")
        print(f"Filing Date: {burry.filing_date}")
        print(f"Total Value: ${burry.total_value:,}")
        print(f"\nTop 10 Holdings:")
        for h in burry.holdings[:10]:
            ticker = h.ticker or h.cusip[:6]
            print(f"  {ticker}: ${h.value:,} ({h.pct_portfolio}%)")


if __name__ == "__main__":
    main()
