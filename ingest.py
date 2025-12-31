"""
Image ingestion module for landscape data pipeline.
Fetches images from Unsplash API with location metadata.
"""
import os
import json
import time
import logging
import requests
from dotenv import load_dotenv

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY')


def setup_directories():
    """Create necessary directories if they don't exist."""
    config.IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_location_from_text(text):
    """
    Parse description text for location keywords.
    Returns (location_name, country) tuple.
    """
    if not text:
        return None, None
    
    text_lower = text.lower()
    
    # Check landmarks first (more specific)
    for keyword, location in config.LANDMARK_KEYWORDS.items():
        if keyword in text_lower:
            country = None
            for country_key, country_val in config.COUNTRY_KEYWORDS.items():
                if country_key in text_lower:
                    country = country_val
                    break
            return location, country
    
    # Check countries
    for keyword, country in config.COUNTRY_KEYWORDS.items():
        if keyword in text_lower:
            return None, country
    
    return None, None


def extract_location(image_data):
    """
    Extract location using multiple methods:
    1. Official Unsplash location data
    2. Parse description for landmarks and countries
    """
    location = image_data.get('location', {})
    
    location_name = location.get('name') or location.get('city')
    country = location.get('country')
    
    # Parse description if location data incomplete
    if not location_name or not country:
        description = image_data.get('description') or image_data.get('alt_description') or ''
        parsed_location, parsed_country = extract_location_from_text(description)
        
        if not location_name and parsed_location:
            location_name = parsed_location
        if not country and parsed_country:
            country = parsed_country
    
    return location_name, country


def fetch_images(query, per_page=30, page=1):
    """Fetch images from Unsplash API."""
    url = f'{config.UNSPLASH_BASE_URL}/search/photos'
    params = {
        'query': query,
        'per_page': per_page,
        'page': page,
        'orientation': config.IMAGE_ORIENTATION,
        'client_id': UNSPLASH_ACCESS_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data['results']
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch '{query}': {e}")
        return []


def parse_image_metadata(image_data, query):
    """Extract relevant metadata from API response."""
    location_name, country = extract_location(image_data)
    
    return {
        'id': image_data['id'],
        'image_url': image_data['urls']['regular'],
        'download_url': image_data['urls']['regular'],  # Use regular for faster downloads
        'page_url': image_data['links']['html'],
        'location_name': location_name,
        'country': country,
        'description': image_data.get('description') or image_data.get('alt_description'),
        'photographer_name': image_data['user']['name'],
        'photographer_username': image_data['user']['username'],
        'width': image_data['width'],
        'height': image_data['height'],
        'color': image_data.get('color'),
        'source': 'unsplash',
        'query': query,
        'downloaded': 0
    }


def download_image(image_url, image_id):
    """Download image to local storage."""
    try:
        response = requests.get(image_url, timeout=15)
        response.raise_for_status()
        
        filepath = config.IMAGES_DIR / f"{image_id}.jpg"
        filepath.write_bytes(response.content)
        return True
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {image_id}: {e}")
        return False


def main():
    """Main ingestion pipeline."""
    if not UNSPLASH_ACCESS_KEY:
        logger.error("UNSPLASH_ACCESS_KEY not found in .env file")
        return
    
    logger.info("Starting image ingestion pipeline")
    logger.info(f"Target: {config.TARGET_IMAGE_COUNT} images using {len(config.SEARCH_QUERIES)} queries")
    
    setup_directories()
    
    all_metadata = []
    images_downloaded = 0
    
    for query in config.SEARCH_QUERIES:
        if images_downloaded >= config.TARGET_IMAGE_COUNT:
            break
        
        logger.info(f"Searching: '{query}'")
        time.sleep(config.RATE_LIMIT_DELAY)
        
        images = fetch_images(query, per_page=config.IMAGES_PER_QUERY, page=1)
        
        if not images:
            continue
        
        for img in images:
            metadata = parse_image_metadata(img, query)
            
            success = download_image(metadata['download_url'], metadata['id'])
            if success:
                metadata['downloaded'] = 1
                images_downloaded += 1
                
                loc_info = ""
                if metadata['location_name'] and metadata['country']:
                    loc_info = f" ({metadata['location_name']}, {metadata['country']})"
                elif metadata['country']:
                    loc_info = f" ({metadata['country']})"
                elif metadata['location_name']:
                    loc_info = f" ({metadata['location_name']})"
                
                if images_downloaded % 20 == 0:
                    logger.info(f"Progress: {images_downloaded}/{config.TARGET_IMAGE_COUNT}{loc_info}")
            
            all_metadata.append(metadata)
            
            if images_downloaded >= config.TARGET_IMAGE_COUNT:
                break
    
    # Save metadata
    with open(config.RAW_METADATA_FILE, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    
    # Summary
    with_location = sum(1 for m in all_metadata if m['location_name'])
    with_country = sum(1 for m in all_metadata if m['country'])
    with_either = sum(1 for m in all_metadata if m['location_name'] or m['country'])
    
    logger.info(f"Ingestion complete: {images_downloaded} images downloaded")
    logger.info(f"Metadata saved: {config.RAW_METADATA_FILE}")
    logger.info(f"Location coverage: {with_either}/{len(all_metadata)} ({with_either/len(all_metadata)*100:.1f}%)")
    
    return all_metadata


if __name__ == '__main__':
    main()
