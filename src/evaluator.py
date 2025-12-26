"""
Evaluation and scoring module.
Implements strict rejection logic for URLs and contacts.
"""
from typing import Dict, List, Set
from urllib.parse import urlparse
import re


# URL scoring threshold
URL_FETCH_THRESHOLD = 40

# Contact confidence threshold
CONTACT_CONFIDENCE_THRESHOLD = 50


def validate_domain_match(url: str, target_domain: str) -> bool:
    """Check if URL belongs to target domain."""
    parsed = urlparse(url)
    url_domain = parsed.netloc.lower().replace('www.', '')
    target_clean = target_domain.lower().replace('www.', '')
    
    return target_clean in url_domain or url_domain in target_clean


def should_fetch_url(candidate: Dict, target_domain: str) -> bool:
    """
    Determine if a URL candidate should be fetched.
    Returns True only if score ≥ URL_FETCH_THRESHOLD and domain matches.
    """
    score = candidate.get('relevance_score', 0)
    
    if score < URL_FETCH_THRESHOLD:
        return False
    
    url = candidate.get('url', '')
    if not validate_domain_match(url, target_domain):
        return False
    
    return True


def calculate_contact_confidence(
    contact: Dict,
    evidence_urls: List[str],
    company_domain: str
) -> int:
    """
    Calculate confidence score (0-100) for a contact.
    Accept only if score ≥ CONTACT_CONFIDENCE_THRESHOLD.
    """
    score = 0
    reasons = []
    
    email = contact.get('email', '')
    phone = contact.get('phone', '')
    role = contact.get('role', '')
    
    # Email on official domain → +25
    if email:
        email_domain = email.split('@')[1].lower() if '@' in email else ''
        company_domain_clean = company_domain.lower().replace('www.', '')
        if company_domain_clean in email_domain or email_domain in company_domain_clean:
            score += 25
            reasons.append("Email on official domain")
    
    # Phone on official site → +15
    if phone:
        score += 15
        reasons.append("Phone found on official site")
    
    # Role explicitly stated → +25
    if role:
        score += 25
        reasons.append("Role explicitly stated")
    
    # Appears on ≥2 pages → +20
    if len(evidence_urls) >= 2:
        score += 20
        reasons.append(f"Found on {len(evidence_urls)} pages")
    
    # Cross-source confirmation → +15
    # (This would require tracking sources, simplified for now)
    if len(evidence_urls) >= 2:
        score += 15
        reasons.append("Cross-source confirmation")
    
    contact['confidence'] = score
    contact['confidence_reasons'] = reasons
    
    return score


def should_accept_contact(contact: Dict) -> bool:
    """
    Determine if a contact should be accepted.
    Returns True only if confidence ≥ CONTACT_CONFIDENCE_THRESHOLD.
    """
    confidence = contact.get('confidence', 0)
    return confidence >= CONTACT_CONFIDENCE_THRESHOLD


def merge_contacts(contacts: List[Dict]) -> List[Dict]:
    """
    Merge duplicate contacts based on email or phone.
    Aggregates evidence URLs.
    """
    # Group by email or phone
    email_map: Dict[str, Dict] = {}
    phone_map: Dict[str, Dict] = {}
    
    for contact in contacts:
        email = contact.get('email', '').lower()
        phone = contact.get('phone', '')
        
        # Normalize phone
        if phone:
            phone = re.sub(r'[\s\-\(\)]', '', phone)
        
        merged = None
        
        # Check email match
        if email and email in email_map:
            merged = email_map[email]
        # Check phone match
        elif phone and phone in phone_map:
            merged = phone_map[phone]
        
        if merged:
            # Merge evidence URLs
            existing_urls = set(merged.get('evidence_urls', []))
            new_urls = set(contact.get('evidence_urls', []))
            merged['evidence_urls'] = list(existing_urls | new_urls)
            
            # Update role if more specific
            if contact.get('role') and not merged.get('role'):
                merged['role'] = contact['role']
            
            # Merge email/phone if missing
            if email and not merged.get('email'):
                merged['email'] = email
            if phone and not merged.get('phone'):
                merged['phone'] = phone
        else:
            # New contact
            contact['evidence_urls'] = contact.get('evidence_urls', [])
            if email:
                email_map[email] = contact
            if phone:
                phone_map[phone] = contact
    
    # Return all unique contacts
    all_contacts = list(email_map.values())
    seen = set()
    unique_contacts = []
    
    for contact in all_contacts:
        email = contact.get('email', '').lower()
        phone = contact.get('phone', '')
        if phone:
            phone = re.sub(r'[\s\-\(\)]', '', phone)
        
        key = (email, phone)
        if key not in seen:
            seen.add(key)
            unique_contacts.append(contact)
    
    return unique_contacts

