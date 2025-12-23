import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()

ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")

async def fetch_page(
    session: aiohttp.ClientSession,
    url: str,
    js_render: bool = False
) -> str:
    """
    Async fetch page using ZenRows API.
    
    Args:
        session: aiohttp session
        url: URL to fetch
        js_render: Whether to use JS rendering (default False for search results)
    
    Returns:
        HTML content or None if fetch failed
    """
    if not ZENROWS_API_KEY:
        raise Exception("ZENROWS_API_KEY not found in .env")

    params = {
        "apikey": ZENROWS_API_KEY,
        "js_render": "true" if js_render else "false",
        "url": url
    }
    
    if js_render:
        params["wait"] = 2000

    try:
        async with session.get(
            "https://api.zenrows.com/v1/",
            params=params,
            timeout=aiohttp.ClientTimeout(total=60)
        ) as response:
            if response.status == 404:
                return None  # Return None for 404s, we'll suppress logs
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientError:
        return None  # Suppress noisy errors
