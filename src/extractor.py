"""
Strict extraction module.
Extracts emails, phones, and roles with aggressive validation.
NEVER guesses emails or roles.
"""
import re
from typing import List, Dict, Set
from bs4 import BeautifulSoup
from urllib.parse import urlparse


# Personal email domains to reject
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "yahoo.co.in", "yahoo.co.uk", "hotmail.co.uk", "live.com",
    "msn.com", "aol.com", "icloud.com", "protonmail.com",
    "mail.com", "yandex.com", "zoho.com", "rediffmail.com",
    "rediff.com", "inbox.com", "gmx.com"
}

# Role keywords (explicit mentions only)
ROLE_KEYWORDS = {
    "pms": ["portfolio manager", "portfolio management", "pms"],
    "insurance agent": ["insurance agent", "insurance advisor", "insurance consultant"],
    "ifa": ["independent financial advisor", "ifa", "financial advisor"],
    "mutual fund": ["mutual fund", "mutual fund manager", "amc", "asset management company"],
    "investment advisor": ["investment advisor", "investment adviser"]
}

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,4}\)?[\s-]?)?\d{3,4}[\s-]?\d{4}")


def is_business_email(email: str) -> bool:
    """Check if email is a business email (not personal domain)."""
    if not email or '@' not in email:
        return False
    
    domain = email.split('@')[1].lower()
    return domain not in PERSONAL_EMAIL_DOMAINS


def normalize_phone(phone: str) -> str:
    """Normalize phone number."""
    normalized = re.sub(r'[\s\-\(\)]', '', phone)
    return normalized


def is_valid_phone(phone: str) -> bool:
    """Validate phone number (reject junk)."""
    normalized = normalize_phone(phone)
    
    # Too short
    if len(normalized) < 7:
        return False
    
    # All zeros
    if normalized.replace('+', '').replace('0', '') == '':
        return False
    
    # All same digit
    if len(set(normalized.replace('+', ''))) == 1:
        return False
    
    return True


def extract_emails(html: str, company_domain: str = "") -> List[str]:
    """
    Extract business emails from HTML.
    Only returns emails on official domain or business domains.
    """
    # Find all email matches
    email_matches = EMAIL_REGEX.findall(html)
    
    # Filter to business emails only
    business_emails = []
    for email in email_matches:
        email_lower = email.lower()
        
        # Reject personal domains
        if not is_business_email(email_lower):
            continue
        
        # If company domain specified, prefer emails on that domain
        if company_domain:
            email_domain = email_lower.split('@')[1]
            company_domain_clean = company_domain.lower().replace('www.', '')
            if company_domain_clean in email_domain or email_domain in company_domain_clean:
                business_emails.append(email_lower)
        else:
            business_emails.append(email_lower)
    
    return sorted(set(business_emails))


def extract_phones(html: str) -> List[str]:
    """
    Extract phone numbers from HTML.
    Validates and normalizes.
    """
    phone_matches = PHONE_REGEX.findall(html)
    
    valid_phones = []
    for phone in phone_matches:
        phone_clean = phone.strip()
        if is_valid_phone(phone_clean):
            normalized = normalize_phone(phone_clean)
            valid_phones.append(normalized)
    
    return sorted(set(valid_phones))


def extract_explicit_role(text: str) -> str:
    """
    Extract explicit role/designation from text.
    Returns empty string if no role found.
    DO NOT infer roles.
    """
    text_lower = text.lower()
    
    # Check for explicit role mentions
    for role_type, keywords in ROLE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                return role_type
    
    return ""


def extract_company_name(html: str) -> str:
    """Extract company name from HTML (title tag)."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try title tag
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Clean up common title suffixes
        title_text = re.sub(r'\s*[-|]\s*(Home|Welcome|Official).*$', '', title_text, flags=re.IGNORECASE)
        if title_text:
            return title_text
    
    # Try h1
    h1_tag = soup.find('h1')
    if h1_tag:
        h1_text = h1_tag.get_text(strip=True)
        if h1_text and len(h1_text) < 100:  # Reasonable length
            return h1_text
    
    return ""


def extract_contacts_from_page(
    html: str,
    url: str,
    company_domain: str = ""
) -> List[Dict]:
    """
    Extract contacts from a single page.
    Returns list of contact dicts with:
    - email (if found)
    - phone (if found)
    - role (if explicitly stated)
    - evidence_urls: [url]
    
    NEVER guesses emails or roles.
    """
    contacts = []
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    
    # Extract emails
    emails = extract_emails(html, company_domain)
    
    # Extract phones
    phones = extract_phones(html)
    
    # Extract explicit role from page text
    role = extract_explicit_role(text)
    
    # Create contact entries
    # Priority: email+phone > email > phone
    # Only create contacts if we have at least email or phone
    
    if emails and phones:
        # Pair emails with phones (one-to-one, best effort)
        for i, email in enumerate(emails):
            phone = phones[i] if i < len(phones) else ''
            contact = {
                'email': email,
                'phone': phone,
                'role': role,
                'evidence_urls': [url]
            }
            contacts.append(contact)
        
        # Add remaining phones if any
        if len(phones) > len(emails):
            for phone in phones[len(emails):]:
                contact = {
                    'email': '',
                    'phone': phone,
                    'role': role,
                    'evidence_urls': [url]
                }
                contacts.append(contact)
    elif emails:
        # Only emails
        for email in emails:
            contact = {
                'email': email,
                'phone': '',
                'role': role,
                'evidence_urls': [url]
            }
            contacts.append(contact)
    elif phones:
        # Only phones
        for phone in phones:
            contact = {
                'email': '',
                'phone': phone,
                'role': role,
                'evidence_urls': [url]
            }
            contacts.append(contact)
    
    # If we have role but no email/phone, don't create contact
    # (we need at least one contact method)
    
    return contacts

