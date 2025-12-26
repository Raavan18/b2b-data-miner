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
    '"{company_name}" contact email',
    '"{company_name}" registered office phone'
]

# Relevance keywords for scoring
CONTACT_KEYWORDS = {'contact', 'about', 'leadership', 'team', 'management', 'email', 'phone'}
ROLE_KEYWORDS = {'pms', 'insurance agent', 'investment advisor', 'ifa', 'mutual fund', 'portfolio manager'}

def generate_search_queries(domain: str, company_name: str = "") -> List[str]:
    """Generate search queries for a given domain and company name."""
    queries = []
    for template in SEARCH_QUERIES:
        if '{domain}' in template:
            if domain and '.' in domain:
                query = template.format(domain=domain)
            else:
                continue # Skip domain queries if not valid
        elif '{company_name}' in template:
            if company_name:
                query = template.format(company_name=company_name)
            else:
                continue
        else:
            query = template
        queries.append(query)
    return queries

def parse_google_results(html: str) -> List[Dict]:
    """Parse Google HTML search results using modern selectors."""
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    
    # Modern Google result container selector
    for g in soup.select('div.g, div.MjjYud'):
        link = g.select_one('a[href]')
        title = g.select_one('h3')
        snippet = g.select_one('div.VwiC3b, span.st')
        
        if link and title:
            url = link['href']
            if url.startswith('http') and 'google.com' not in url:
                results.append({
                    'url': url,
                    'title': title.get_text(strip=True),
                    'snippet': snippet.get_text(strip=True) if snippet else '',
                    'source': 'google'
                })
    return results

def score_url_candidate(candidate: Dict, target_domain: str) -> int:
    """Score a URL candidate based on relevance and domain matching."""
    score = 0
    url = candidate.get('url', '').lower()
    combined_text = f"{candidate.get('title', '')} {candidate.get('snippet', '')}".lower()
    
    # Loose Domain Check: Allow subdomains or similar matches
    if target_domain:
        clean_target = target_domain.lower().replace('www.', '')
        if clean_target in url:
            score += 50
    
    # Keywords match
    if any(k in combined_text for k in CONTACT_KEYWORDS): score += 30
    if '@' in combined_text or 'phone' in combined_text: score += 40
    if any(k in combined_text for k in ROLE_KEYWORDS): score += 40
    
    return score

async def search_google(session: aiohttp.ClientSession, query: str) -> List[Dict]:
    encoded_query = quote_plus(query)
    search_url = f"https://www.google.com/search?q={encoded_query}&num=10"
    html = await fetch_page(session, search_url, js_render=True) # JS Render helps bypass simple bot checks
    return parse_google_results(html) if html else []

async def search_duckduckgo(session: aiohttp.ClientSession, query: str) -> List[Dict]:
    encoded_query = quote_plus(query)
    search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
    html = await fetch_page(session, search_url, js_render=False)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    for div in soup.find_all('div', class_='result'):
        link = div.find('a', class_='result__a')
        if link and link.get('href'):
            results.append({
                'url': link['href'],
                'title': link.get_text(strip=True),
                'snippet': div.find('a', class_='result__snippet').get_text(strip=True) if div.find('a', class_='result__snippet') else '',
                'source': 'duckduckgo'
            })
    return results

async def discover_candidate_urls(session: aiohttp.ClientSession, domain: str, company_name: str = "") -> List[Dict]:
    queries = generate_search_queries(domain, company_name)
    all_candidates = []
    for query in queries:
        all_candidates.extend(await search_google(session, query))
        all_candidates.extend(await search_duckduckgo(session, query))
    
    seen_urls = set()
    deduplicated = []
    for c in all_candidates:
        if c['url'] not in seen_urls:
            seen_urls.add(c['url'])
            c['relevance_score'] = score_url_candidate(c, domain)
            deduplicated.append(c)
    
    deduplicated.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
    return deduplicated