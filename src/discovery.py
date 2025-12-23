"""
Search-based discovery module.
Uses hybrid search (Google + DuckDuckGo) to find candidate URLs.
DO NOT crawl websites directly.
"""
import re
from typing import List, Dict, Set
from urllib.parse import urlparse, urljoin, quote_plus
import aiohttp
from bs4 import BeautifulSoup

from src.zenrows_client import fetch_page


# Search query templates
SEARCH_QUERIES = [
    'site:{domain} "contact us"',
    'site:{domain} "registered office"',
    'site:{domain} "email"',
    'site:{domain} "phone"',
    'site:{domain} "about us"',
    'site:{domain} "management committee"',
    'site:{domain} "AMFI"',
    '"Association of Mutual Funds in India" email',
    '"Association of Mutual Funds in India" contact'
]

# Relevance keywords for scoring
CONTACT_KEYWORDS = {'contact', 'about', 'leadership', 'team', 'management', 'email', 'phone'}
ROLE_KEYWORDS = {'pms', 'insurance agent', 'investment advisor', 'ifa', 'mutual fund', 'portfolio manager'}


def generate_search_queries(domain: str, company_name: str = "") -> List[str]:
    """Generate search queries for a given domain and company name."""
    queries = []
    for template in SEARCH_QUERIES:
        if '{domain}' in template:
            query = template.format(domain=domain)
        elif '{company_name}' in template:
            if company_name:
                query = template.format(company_name=company_name)
            else:
                continue  # Skip company name queries if name not available
        else:
            query = template
        queries.append(query)
    return queries


def parse_google_results(html: str) -> List[Dict]:
    """
    Parse Google HTML search results.
    Returns list of {url, snippet, title} dicts.
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Google result containers
    result_divs = soup.find_all('div', class_=lambda x: x and ('g' in x.lower() or 'result' in x.lower()))
    
    for div in result_divs:
        # Extract URL
        link = div.find('a', href=True)
        if not link:
            continue
        
        url = link.get('href', '')
        if not url.startswith('http'):
            continue
        
        # Extract title
        title_elem = div.find('h3') or div.find('a')
        title = title_elem.get_text(strip=True) if title_elem else ''
        
        # Extract snippet
        snippet_elem = div.find('span', class_=lambda x: x and ('st' in x.lower() or 'snippet' in x.lower()))
        if not snippet_elem:
            # Try alternative snippet locations
            snippet_elem = div.find('div', class_=lambda x: x and ('s' in x.lower() or 'snippet' in x.lower()))
        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ''
        
        if url:
            results.append({
                'url': url,
                'title': title,
                'snippet': snippet,
                'source': 'google'
            })
    
    return results


def parse_duckduckgo_results(html: str) -> List[Dict]:
    """
    Parse DuckDuckGo HTML search results.
    Returns list of {url, snippet, title} dicts.
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # DuckDuckGo result containers
    result_divs = soup.find_all('div', class_=lambda x: x and ('result' in x.lower()))
    
    for div in result_divs:
        # Extract URL
        link = div.find('a', class_=lambda x: x and ('result__a' in x.lower() or 'result__url' in x.lower()))
        if not link:
            link = div.find('a', href=True)
        
        if not link:
            continue
        
        url = link.get('href', '')
        if not url.startswith('http'):
            continue
        
        # Extract title
        title = link.get_text(strip=True)
        
        # Extract snippet
        snippet_elem = div.find('a', class_=lambda x: x and ('result__snippet' in x.lower()))
        if not snippet_elem:
            snippet_elem = div.find('div', class_=lambda x: x and ('snippet' in x.lower()))
        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ''
        
        if url:
            results.append({
                'url': url,
                'title': title,
                'snippet': snippet,
                'source': 'duckduckgo'
            })
    
    return results


def score_url_candidate(candidate: Dict, target_domain: str) -> int:
    """
    Score a URL candidate based on relevance.
    Returns score (0-100+).
    Threshold to fetch: ≥ 40
    """
    score = 0
    url = candidate.get('url', '').lower()
    snippet = candidate.get('snippet', '').lower()
    title = candidate.get('title', '').lower()
    combined_text = f"{title} {snippet}".lower()
    
    # Domain match is mandatory
    parsed_url = urlparse(url)
    candidate_domain = parsed_url.netloc.lower()
    target_domain_clean = target_domain.lower().replace('www.', '')
    candidate_domain_clean = candidate_domain.replace('www.', '')
    
    if target_domain_clean not in candidate_domain_clean and candidate_domain_clean not in target_domain_clean:
        return 0  # Reject if domain doesn't match
    
    # Contact/about/leadership keywords → +30
    if any(keyword in combined_text for keyword in CONTACT_KEYWORDS):
        score += 30
    
    # Mentions email or phone in snippet → +40
    if 'email' in combined_text or 'phone' in combined_text or '@' in combined_text:
        score += 40
    
    # Mentions role keywords → +40
    if any(keyword in combined_text for keyword in ROLE_KEYWORDS):
        score += 40
    
    return score


async def search_google(session: aiohttp.ClientSession, query: str) -> List[Dict]:
    """Search Google using ZenRows."""
    # Google search URL (URL-encode query)
    encoded_query = quote_plus(query)
    search_url = f"https://www.google.com/search?q={encoded_query}&num=10"
    
    html = await fetch_page(session, search_url, js_render=False)
    if not html:
        return []
    
    results = parse_google_results(html)
    return results


async def search_duckduckgo(session: aiohttp.ClientSession, query: str) -> List[Dict]:
    """Search DuckDuckGo using ZenRows."""
    # DuckDuckGo search URL (URL-encode query)
    encoded_query = quote_plus(query)
    search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
    
    html = await fetch_page(session, search_url, js_render=False)
    if not html:
        return []
    
    results = parse_duckduckgo_results(html)
    return results


async def discover_candidate_urls(
    session: aiohttp.ClientSession,
    domain: str,
    company_name: str = ""
) -> List[Dict]:
    """
    Discover candidate URLs using hybrid search.
    Returns deduplicated list of candidates with scores.
    """
    queries = generate_search_queries(domain, company_name)
    all_candidates = []
    
    # Search both engines
    for query in queries:
        # Google search
        google_results = await search_google(session, query)
        all_candidates.extend(google_results)
        
        # DuckDuckGo search
        ddg_results = await search_duckduckgo(session, query)
        all_candidates.extend(ddg_results)
    
    # Deduplicate by URL
    seen_urls: Set[str] = set()
    deduplicated = []
    
    for candidate in all_candidates:
        url = candidate['url']
        if url not in seen_urls:
            seen_urls.add(url)
            # Score the candidate
            score = score_url_candidate(candidate, domain)
            candidate['relevance_score'] = score
            deduplicated.append(candidate)
    
    # Sort by score descending
    deduplicated.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
    
    return deduplicated

