#!/usr/bin/env python3
"""
CLI entrypoint for B2B lead intelligence system.
Usage: python run.py <domain> [company_name]
Output: JSON to stdout (n8n-compatible)
"""
import sys
import json
import asyncio
from src.main import run_company_intelligence


def main():
    if len(sys.argv) < 2:
        error_result = {
            "error": "Usage: python run.py <domain> [company_name]",
            "company_name": "",
            "company_domain": "",
            "contacts": [],
            "meta": {
                "candidates_discovered": 0,
                "urls_fetched": 0,
                "contacts_extracted": 0,
                "contacts_accepted": 0,
                "discovery_urls": [],
                "fetch_urls": []
            }
        }
        print(json.dumps(error_result, indent=2), file=sys.stderr)
        sys.exit(1)
    
    domain = sys.argv[1]
    company_name = sys.argv[2] if len(sys.argv) > 2 else ""
    
    try:
        result = asyncio.run(run_company_intelligence(domain, company_name))
        # Output JSON only (no prints)
        print(json.dumps(result, indent=2))
        sys.exit(0)
    except Exception as e:
        error_result = {
            "error": str(e),
            "company_name": company_name,
            "company_domain": domain,
            "contacts": [],
            "meta": {
                "candidates_discovered": 0,
                "urls_fetched": 0,
                "contacts_extracted": 0,
                "contacts_accepted": 0,
                "discovery_urls": [],
                "fetch_urls": []
            }
        }
        print(json.dumps(error_result, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()

