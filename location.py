"""Location extraction from Unsplash metadata and free-text descriptions."""
import re

import config


def _find_keywords(text, keywords):
    """Return keyword matches as (position, keyword), earliest first.

    Matches whole words only, so 'usa' does not match inside 'Jerusalem'.
    """
    matches = []
    for keyword in keywords:
        match = re.search(rf'\b{re.escape(keyword)}\b', text)
        if match:
            matches.append((match.start(), keyword))
    return sorted(matches)


def extract_location_from_text(text):
    if not text:
        return None, None

    text_lower = text.lower()
    countries = _find_keywords(text_lower, config.COUNTRY_KEYWORDS)
    country = config.COUNTRY_KEYWORDS[countries[0][1]] if countries else None

    for _, keyword in _find_keywords(text_lower, config.LANDMARK_KEYWORDS):
        name, landmark_country = config.LANDMARK_KEYWORDS[keyword]
        # Skip landmarks contradicted by the text, e.g. an Indonesian "crater lake"
        if country and country != landmark_country:
            continue
        return name, landmark_country

    return None, country


def extract_location(image_data):
    """Prefer Unsplash's own location data and fall back to the description."""
    location = image_data.get('location') or {}
    location_name = location.get('name') or location.get('city')
    country = location.get('country')

    if not location_name or not country:
        description = image_data.get('description') or image_data.get('alt_description') or ''
        parsed_location, parsed_country = extract_location_from_text(description)
        location_name = location_name or parsed_location
        country = country or parsed_country

    return location_name, country


def is_text_derived(location_name):
    """True when a stored location could only have come from keyword matching."""
    landmark_names = {name for name, _ in config.LANDMARK_KEYWORDS.values()}
    return location_name is None or location_name in landmark_names
