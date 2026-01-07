"""Image ingestion from Unsplash API."""
import os
import json
import time
import logging
import requests
from dotenv import load_dotenv

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY')


def setup_directories():
    config.IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_location_from_text(text):
    if not text:
        return None, None
    
    text_lower = text.lower()
    
    for keyword, location in config.LANDMARK_KEYWORDS.items():
        if keyword in text_lower:
            country = next(
                (v for k, v in config.COUNTRY_KEYWORDS.items() if k in text_lower),
                None
            )
            return location, country
    
    for keyword, country in config.COUNTRY_KEYWORDS.items():
        if keyword in text_lower:
            return None, country
    
    return None, None


def extract_location(image_data):
    location = image_data.get('location', {})
    location_name = location.get('name') or location.get('city')
    country = location.get('country')
    
    if not location_name or not country:
        description = image_data.get('description') or image_data.get('alt_description') or ''
        parsed_location, parsed_country = extract_location_from_text(description)
        location_name = location_name or parsed_location
        country = country or parsed_country
    
    return location_name, country


def fetch_images(query, per_page=30, page=1):
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
        return response.json()['results']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch '{query}': {e}")
        return []


def parse_image_metadata(image_data, query):
    location_name, country = extract_location(image_data)
    
    return {
        'id': image_data['id'],
        'image_url': image_data['urls']['regular'],
        'download_url': image_data['urls']['full'],
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
    if not UNSPLASH_ACCESS_KEY:
        logger.error("UNSPLASH_ACCESS_KEY not found")
        return
    
    logger.info(f"Starting ingestion | Target: {config.TARGET_IMAGE_COUNT} images")
    setup_directories()
    
    all_metadata = []
    images_downloaded = 0
    
    for query in config.SEARCH_QUERIES:
        if images_downloaded >= config.TARGET_IMAGE_COUNT:
            break
        
        logger.info(f"Searching: '{query}'")
        time.sleep(config.RATE_LIMIT_DELAY)
        
        images = fetch_images(query, per_page=config.IMAGES_PER_QUERY, page=1)
        
        for img in images:
            metadata = parse_image_metadata(img, query)
            
            if download_image(metadata['download_url'], metadata['id']):
                metadata['downloaded'] = 1
                images_downloaded += 1
                
                if images_downloaded % 20 == 0:
                    logger.info(f"Progress: {images_downloaded}/{config.TARGET_IMAGE_COUNT}")
            
            all_metadata.append(metadata)
            
            if images_downloaded >= config.TARGET_IMAGE_COUNT:
                break
    
    with open(config.RAW_METADATA_FILE, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    
    with_location = sum(1 for m in all_metadata if m['location_name'] or m['country'])
    logger.info(f"Complete: {images_downloaded} images | Location coverage: {with_location}/{len(all_metadata)}")
    
    return all_metadata


if __name__ == '__main__':
    main()
