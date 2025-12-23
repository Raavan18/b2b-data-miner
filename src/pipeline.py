"""
End-to-end lead mining pipeline.
This is the ONLY public entrypoint for CLI, API, and n8n.
"""

import asyncio
from typing import Dict, List

import aiohttp

from src.discovery import discover_candidate_urls
from src.evaluator import (
    should_fetch_url,
    merge_contacts,
    calculate_contact_confidence,
    should_accept_contact,
)
from src.extractor import extract_contacts_from_page
from src.zenrows_client import fetch_page


async def _run_pipeline(domain: str) -> Dict:
    results = {
        "domain": domain,
        "accepted_contacts": [],
        "rejected_contacts": [],
        "fetched_urls": [],
        "skipped_urls": [],
    }

    async with aiohttp.ClientSession() as session:
        # 1️⃣ Discover URLs
        candidates = await discover_candidate_urls(session, domain)

        contacts = []

        # 2️⃣ Filter + fetch URLs
        for candidate in candidates:
            url = candidate["url"]

            if not should_fetch_url(candidate, domain):
                results["skipped_urls"].append(url)
                continue

            html = await fetch_page(session, url, js_render=False)
            if not html:
                results["skipped_urls"].append(url)
                continue

            results["fetched_urls"].append(url)

            # 3️⃣ Extract contacts
            page_contacts = extract_contacts_from_page(
                html=html,
                url=url,
                company_domain=domain,
            )

            contacts.extend(page_contacts)

        # 4️⃣ Merge duplicates
        merged = merge_contacts(contacts)

        # 5️⃣ Score + accept
        for contact in merged:
            confidence = calculate_contact_confidence(
                contact,
                contact.get("evidence_urls", []),
                domain,
            )

            if should_accept_contact(contact):
                results["accepted_contacts"].append(contact)
            else:
                results["rejected_contacts"].append(contact)

    return results


def run(domain: str) -> Dict:
    """
    Sync wrapper for API / CLI / n8n
    """
    return asyncio.run(_run_pipeline(domain))
