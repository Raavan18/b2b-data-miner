from urllib.parse import urljoin

INTENT_PATHS = [
    "/",
    "/about",
    "/about-us",
    "/team",
    "/leadership",
    "/management",
    "/contact"
]

def generate_intent_urls(base_url: str):
    urls = []
    for path in INTENT_PATHS:
        urls.append(urljoin(base_url, path))
    return list(set(urls))
