"""
SEC 13F Filing Scraper for InvestorInsight
Scrapes 13F-HR filings from SEC EDGAR to track superinvestor holdings.
"""

import requests
import json
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEC_EDGAR_BASE = "https://www.sec.gov"
SEC_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"

HEADERS = {
    "User-Agent": "InvestorInsight Research Bot (contact@investorinsight.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, application/xml"
}

# 77 Superinvestors (matching Dataroma)
SUPERINVESTORS = {
    # Legendary Value Investors
    "1067983": {"name": "Warren Buffett", "firm": "Berkshire Hathaway Inc"},
    "1336528": {"name": "Bill Ackman", "firm": "Pershing Square Capital Management"},
    "1649339": {"name": "Michael Burry", "firm": "Scion Asset Management LLC"},
    "1061768": {"name": "Seth Klarman", "firm": "Baupost Group LLC"},
    "1040273": {"name": "Daniel Loeb", "firm": "Third Point LLC"},
    "1345471": {"name": "Nelson Peltz", "firm": "Trian Fund Management LP"},
    "1709323": {"name": "Li Lu", "firm": "Himalaya Capital Management LLC"},
    "1173334": {"name": "Mohnish Pabrai", "firm": "Pabrai Investment Funds"},
    "1549341": {"name": "Guy Spier", "firm": "Aquamarine Capital Management LLC"},
    "921669": {"name": "Carl Icahn", "firm": "Icahn Capital LP"},
    "1079114": {"name": "David Einhorn", "firm": "Greenlight Capital Inc"},
    "1656456": {"name": "David Tepper", "firm": "Appaloosa Management LP"},
    
    # Large Fund Managers
    "1647251": {"name": "Chris Hohn", "firm": "TCI Fund Management Ltd"},
    "1167483": {"name": "Chase Coleman", "firm": "Tiger Global Management LLC"},
    "1350694": {"name": "Ray Dalio", "firm": "Bridgewater Associates LP"},
    "1510387": {"name": "Joel Greenblatt", "firm": "Gotham Asset Management LLC"},
    "1096343": {"name": "Tom Gayner", "firm": "Markel Corporation"},
    "1112520": {"name": "Chuck Akre", "firm": "Akre Capital Management LLC"},
    "1766596": {"name": "Pat Dorsey", "firm": "Dorsey Asset Management LLC"},
    "1802994": {"name": "Jeffrey Ubben", "firm": "Inclusive Capital Partners LP"},
    "1657335": {"name": "Leon Cooperman", "firm": "Omega Advisors Inc"},
    "1536411": {"name": "Stanley Druckenmiller", "firm": "Duquesne Family Office LLC"},
    "1048445": {"name": "Paul Singer", "firm": "Elliott Investment Management LP"},
    "1569205": {"name": "Terry Smith", "firm": "Fundsmith LLP"},
    "949509": {"name": "Howard Marks", "firm": "Oaktree Capital Management LP"},
    
    # Institutional Investors
    "1618584": {"name": "Bill & Melinda Gates Foundation", "firm": "Gates Foundation Trust"},
    "315066": {"name": "Dodge & Cox", "firm": "Dodge & Cox"},
    "1000275": {"name": "First Eagle Investment", "firm": "First Eagle Investment Management"},
    "1279708": {"name": "Polen Capital", "firm": "Polen Capital Management"},
    
    # Value Investors
    "1568820": {"name": "AKO Capital", "firm": "AKO Capital LLP"},
    "1537996": {"name": "AltaRock Partners", "firm": "AltaRock Partners"},
    "1547230": {"name": "Bill Miller", "firm": "Miller Value Partners"},
    "908809": {"name": "Bill Nygren", "firm": "Oakmark Select Fund"},
    "1056831": {"name": "Bruce Berkowitz", "firm": "Fairholme Capital"},
    "1772460": {"name": "Bryan Lawrence", "firm": "Oakcliff Capital"},
    "1455099": {"name": "Charles Bobrinskoy", "firm": "Ariel Focus Fund"},
    "1008540": {"name": "Christopher Bloomstran", "firm": "Semper Augustus"},
    "816345": {"name": "Christopher Davis", "firm": "Davis Advisors"},
    "1766199": {"name": "Clifford Sosin", "firm": "CAS Investment Partners"},
    "1512613": {"name": "David Abrams", "firm": "Abrams Capital Management"},
    "1576280": {"name": "David Katz", "firm": "Matrix Asset Advisors"},
    "1033896": {"name": "David Rolfe", "firm": "Wedgewood Partners"},
    "1697189": {"name": "Dennis Hong", "firm": "ShawSpring Partners"},
    "1715541": {"name": "Duan Yongping", "firm": "H&H International Investment"},
    "927855": {"name": "Francis Chou", "firm": "Chou Associates"},
    "1105838": {"name": "Francois Rochon", "firm": "Giverny Capital"},
    "1132439": {"name": "Glenn Greenberg", "firm": "Brave Warrior Advisors"},
    "1571047": {"name": "Glenn Welling", "firm": "Engaged Capital"},
    "1536253": {"name": "Greenhaven Associates", "firm": "Greenhaven Associates"},
    "1650274": {"name": "Greg Alexander", "firm": "Conifer Management"},
    "1045446": {"name": "Harry Burn", "firm": "Sound Shore"},
    "1714475": {"name": "Hillman Value Fund", "firm": "Hillman Capital Management"},
    "1001045": {"name": "Jensen Investment", "firm": "Jensen Investment Management"},
    "1438809": {"name": "John Armitage", "firm": "Egerton Capital"},
    "924245": {"name": "John Rogers", "firm": "Ariel Appreciation Fund"},
    "1758311": {"name": "Josh Tarasoff", "firm": "Greenlea Lane Capital"},
    "920153": {"name": "Kahn Brothers", "firm": "Kahn Brothers Group"},
    "916340": {"name": "Lee Ainslie", "firm": "Maverick Capital"},
    "1533333": {"name": "Lindsell Train", "firm": "Lindsell Train"},
    "855266": {"name": "Mairs & Power", "firm": "Mairs & Power Growth Fund"},
    "806343": {"name": "Mason Hawkins", "firm": "Longleaf Partners"},
    "1108131": {"name": "Meridian Contrarian Fund", "firm": "Meridian Fund"},
    "1603432": {"name": "Norbert Lou", "firm": "Punch Card Management"},
    "1026630": {"name": "Prem Watsa", "firm": "Fairfax Financial Holdings"},
    "1008634": {"name": "Richard Pzena", "firm": "Pzena Investment Management"},
    "732263": {"name": "Robert Olstein", "firm": "Olstein Capital Management"},
    "1559832": {"name": "Robert Vinall", "firm": "RV Capital GmbH"},
    "353011": {"name": "Ruane Cunniff", "firm": "Sequoia Fund"},
    "1639061": {"name": "Samantha McLemore", "firm": "Patient Capital Management"},
    "1082475": {"name": "Sarah Ketterer", "firm": "Causeway Capital Management"},
    "1040199": {"name": "Stephen Mandel", "firm": "Lone Pine Capital"},
    "1045102": {"name": "Steven Romick", "firm": "FPA Crescent Fund"},
    "919065": {"name": "Third Avenue Management", "firm": "Third Avenue Management"},
    "1141360": {"name": "Thomas Russo", "firm": "Gardner Russo & Quinn"},
    "1710176": {"name": "Tom Bancroft", "firm": "Makaira Partners"},
    "1568489": {"name": "Alex Roepers", "firm": "Atlantic Investment Management"},
    "1697085": {"name": "FPA Queens Road", "firm": "FPA Queens Road Small Cap Value"},
}

# CUSIP to ticker mapping
CUSIP_TO_TICKER = {
    "037833": "AAPL", "02079K": "GOOGL", "02079L": "GOOG", "594918": "MSFT",
    "023135": "AMZN", "30303M": "META", "67066G": "NVDA", "88160R": "TSLA",
    "084670": "BRK.B", "060505": "BAC", "46625H": "JPM", "92826C": "V",
    "478160": "JNJ", "931142": "WMT", "742718": "PG", "88579Y": "MA",
    "172967": "C", "254687": "DIS", "459200": "IBM", "713448": "PEP",
    "191216": "KO", "166764": "CVX", "30231G": "XOM", "882508": "TXN",
    "69608A": "PLTR", "717081": "PFE", "406216": "HAL", "550021": "LULU",
}
