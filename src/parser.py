import re
from typing import List, Dict

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
PHONE_REGEX = r"(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,4}\)?[\s-]?)?\d{3,4}[\s-]?\d{4}"

# Common personal email domains to filter out
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "yahoo.co.in", "yahoo.co.uk", "hotmail.co.uk", "live.com",
    "msn.com", "aol.com", "icloud.com", "protonmail.com",
    "mail.com", "yandex.com", "zoho.com", "rediffmail.com"
}

def normalize_phone(phone: str) -> str:
    """Normalize phone number by removing spaces, dashes, parentheses."""
    normalized = re.sub(r'[\s\-\(\)]', '', phone)
    return normalized

def is_valid_phone(phone: str) -> bool:
    """Filter out junk phone numbers."""
    normalized = normalize_phone(phone)
    # Filter out: too short (<7 digits), all zeros, all same digit
    if len(normalized) < 7:
        return False
    if normalized.replace('+', '').replace('0', '') == '':
        return False
    if len(set(normalized.replace('+', ''))) == 1:
        return False
    return True

def is_business_email(email: str) -> bool:
    """Check if email is a business email (not personal domain)."""
    domain = email.split('@')[1].lower() if '@' in email else ''
    return domain not in PERSONAL_EMAIL_DOMAINS

def extract_emails(text: str) -> List[str]:
    emails = re.findall(EMAIL_REGEX, text)
    # Filter to business emails only
    business_emails = [e.lower() for e in emails if is_business_email(e.lower())]
    return sorted(set(business_emails))

def extract_phones(text: str) -> List[str]:
    phones = re.findall(PHONE_REGEX, text)
    # Normalize and filter valid phones
    valid_phones = [normalize_phone(p.strip()) for p in phones if is_valid_phone(p.strip())]
    return sorted(set(valid_phones))

def extract_company_name(html: str) -> str:
    """
    Very basic company name extraction using <title>.
    We will improve this later.
    """
    match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""
PERSONA_KEYWORDS = {
    "PMS": [
        "portfolio manager",
        "fund manager",
        "investment manager"
    ],
    "Insurance Agent": [
        "insurance advisor",
        "insurance agent",
        "insurance consultant"
    ],
    "IFA": [
        "independent financial advisor",
        "financial advisor",
        "wealth advisor"
    ],
    "Mutual Fund": [
        "mutual fund manager",
        "asset management",
        "amc",
        "fund house"
    ],
    "Financial Influencer": [
        "founder",
        "content creator",
        "youtuber",
        "finfluencer"
    ]
}

NAME_TITLE_REGEX = r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)\s*[-,–]\s*([A-Za-z ]{3,80})"

def classify_persona(title: str) -> str:
    title_lower = title.lower()
    for persona, keywords in PERSONA_KEYWORDS.items():
        for keyword in keywords:
            if keyword in title_lower:
                return persona
    return ""

def extract_people_with_designations(text: str):
    """
    Extract (name, designation, persona) tuples
    only when name + title are explicitly present.
    """
    matches = re.findall(NAME_TITLE_REGEX, text)
    results = []

    for name, title in matches:
        persona = classify_persona(title)
        results.append({
            "full_name": name.strip(),
            "designation": title.strip(),
            "persona": persona
        })

    return results
from bs4 import BeautifulSoup

def extract_people_dom(html: str):
    soup = BeautifulSoup(html, "html.parser")
    people = []

    # common team card patterns
    possible_cards = soup.find_all(
        ["div", "section"],
        class_=lambda x: x and any(k in x.lower() for k in ["team", "member", "card", "profile"])
    )

    for card in possible_cards:
        text = card.get_text(" ", strip=True)
        matches = re.findall(NAME_TITLE_REGEX, text)
        for name, title in matches:
            persona = classify_persona(title)
            people.append({
                "full_name": name.strip(),
                "designation": title.strip(),
                "persona": persona,
                "source": "dom"
            })

    return people
ROLE_PRIORITY = {
    "PMS": 5,
    "Mutual Fund": 4,
    "Insurance Agent": 3,
    "IFA": 3,
    "Financial Influencer": 2,
    "": 0
}

def rank_people(people: list):
    """Rank people by persona priority and add confidence scores."""
    for person in people:
        person["priority"] = ROLE_PRIORITY.get(person["persona"], 0)
        # Calculate confidence based on persona match
        if person["persona"]:
            person["confidence"] = 80 if person["priority"] >= 4 else 60
            person["confidence_reason"] = f"Persona matched: {person['persona']}"
        else:
            person["confidence"] = 30
            person["confidence_reason"] = "No persona match found"
    
    ranked = sorted(people, key=lambda x: x["priority"], reverse=True)
    # Remove priority field from final output, keep only required fields
    for person in ranked:
        if "priority" in person:
            del person["priority"]
        if "source" in person:
            del person["source"]
    
    return ranked
