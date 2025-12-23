"""
Main orchestration module for B2B lead intelligence system.
Implements: Discovery → Evaluation → Extraction → Confidence Scoring
"""
import asyncio
import aiohttp
from urllib.parse import urlparse
from typing import Dict, List

from src.discovery import discover_candidate_urls
from src.evaluator import (
    should_fetch_url,
    calculate_contact_confidence,
    should_accept_contact,
    merge_contacts
)
from src.extractor import (
    extract_company_name,
    extract_contacts_from_page
)
from src.zenrows_client import fetch_page


async def process_candidate_url(
    session: aiohttp.ClientSession,
    candidate: Dict,
    company_domain: str,
    all_contacts: List[Dict]
) -> None:
    """
    Process a single candidate URL:
    1. Fetch the page
    2. Extract contacts
    3. Add to all_contacts list
    """
    url = candidate.get('url', '')
    if not url:
        return
    
    # Fetch page (with JS rendering for dynamic sites)
    html = await fetch_page(session, url, js_render=True)
    if not html:
        return
    
    # Extract contacts from page
    page_contacts = extract_contacts_from_page(html, url, company_domain)
    
    # Add to all contacts
    all_contacts.extend(page_contacts)


async def run_company_intelligence(
    domain: str,
    company_name: str = ""
) -> Dict:
    """
    Main intelligence gathering function.
    
    Flow:
    1. Discover candidate URLs via hybrid search
    2. Score and filter URLs (threshold ≥40)
    3. Fetch filtered URLs
    4. Extract contacts from pages
    5. Calculate confidence scores (threshold ≥85)
    6. Return only accepted contacts
    
    Returns:
        JSON-serializable dict with structure:
        {
            "company_name": "",
            "company_domain": "",
            "contacts": [
                {
                    "email": "",
                    "phone": "",
                    "role": "",
                    "confidence": 0,
                    "confidence_reasons": [],
                    "evidence_urls": []
                }
            ],
            "meta": {
                "candidates_discovered": 0,
                "urls_fetched": 0,
                "contacts_extracted": 0,
                "contacts_accepted": 0,
                "discovery_urls": [],
                "fetch_urls": []
            }
        }
    """
    # Initialize result structure
    parsed_url = urlparse(domain if domain.startswith('http') else f'https://{domain}')
    company_domain = parsed_url.netloc or domain
    
    result = {
        "company_name": company_name,
        "company_domain": company_domain,
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
    
    async with aiohttp.ClientSession() as session:
        # STEP 1: Discovery - Find candidate URLs via search
        candidates = await discover_candidate_urls(session, company_domain, company_name)
        result["meta"]["candidates_discovered"] = len(candidates)
        result["meta"]["discovery_urls"] = [c.get('url', '') for c in candidates]
        
        # STEP 2: Evaluation - Filter URLs that meet threshold
        fetch_candidates = [
            c for c in candidates
            if should_fetch_url(c, company_domain)
        ]
        result["meta"]["urls_fetched"] = len(fetch_candidates)
        result["meta"]["fetch_urls"] = [c.get('url', '') for c in fetch_candidates]
        
        # STEP 3: Fetch and Extract - Process filtered URLs
        all_contacts = []
        
        # Process URLs sequentially (accuracy > speed)
        for candidate in fetch_candidates:
            await process_candidate_url(session, candidate, company_domain, all_contacts)
        
        result["meta"]["contacts_extracted"] = len(all_contacts)
        
        # STEP 4: Merge duplicate contacts
        merged_contacts = merge_contacts(all_contacts)
        
        # STEP 5: Calculate confidence scores
        for contact in merged_contacts:
            evidence_urls = contact.get('evidence_urls', [])
            calculate_contact_confidence(contact, evidence_urls, company_domain)
        
        # STEP 6: Filter by confidence threshold
        accepted_contacts = [
            c for c in merged_contacts
            if should_accept_contact(c)
        ]
        result["meta"]["contacts_accepted"] = len(accepted_contacts)
        
        # STEP 7: Fetch company name from first successful page
        if fetch_candidates:
            first_url = fetch_candidates[0].get('url', '')
            html = await fetch_page(session, first_url, js_render=True)
            if html:
                extracted_name = extract_company_name(html)
                if extracted_name:
                    result["company_name"] = extracted_name
        
        # Format final contacts (remove internal fields, ensure JSON-serializable)
        formatted_contacts = []
        for contact in accepted_contacts:
            formatted_contact = {
                "email": contact.get('email', ''),
                "phone": contact.get('phone', ''),
                "role": contact.get('role', ''),
                "confidence": contact.get('confidence', 0),
                "confidence_reasons": contact.get('confidence_reasons', []),
                "evidence_urls": contact.get('evidence_urls', [])
            }
            formatted_contacts.append(formatted_contact)
        
        result["contacts"] = formatted_contacts
    
    return result
