from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import re

app = FastAPI(title="InvestorInsight API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"name": "InvestorInsight API", "status": "online", "version": "v2"}

@app.get("/api/debug-xml")
def debug_xml():
    """See what's in Burry's XML file"""
    try:
        headers = {"User-Agent": "InvestorInsight test@test.com"}
        
        # Burry's filing index
        index_url = "https://www.sec.gov/Archives/edgar/data/1649339/000164933925000007/"
        r = requests.get(index_url, headers=headers, timeout=15)
        
        # Find all links
        all_links = re.findall(r'href="([^"]+)"', r.text)
        
        # Find XML files
        xml_files = [f for f in all_links if f.endswith('.xml')]
        
        # Fetch the info table XML (not primary_doc)
        xml_content = None
        xml_url = None
        for f in xml_files:
            if 'primary_doc' not in f.lower():
                if f.startswith('/'):
                    xml_url = f"https://www.sec.gov{f}"
                elif f.startswith('http'):
                    xml_url = f
                else:
                    xml_url = f"{index_url}{f}"
                r2 = requests.get(xml_url, headers=headers, timeout=15)
                xml_content = r2.text
                break
        
        if xml_content:
            # Find unique tags
            tags = list(set(re.findall(r'<([a-zA-Z0-9_:]+)', xml_content)))
            
            return {
                "status": "ok",
                "xml_url": xml_url,
                "xml_files": xml_files,
                "content_length": len(xml_content),
                "tags_found": sorted(tags),
                "preview": xml_content[:2000]
            }
        else:
            return {"status": "error", "message": "No XML found", "xml_files": xml_files}
            
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "trace": traceback.format_exc()}
